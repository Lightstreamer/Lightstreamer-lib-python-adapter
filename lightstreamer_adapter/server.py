
import sys
import socket
import queue
import logging
import time
import multiprocessing
from threading import Thread, Event, Lock, RLock
from abc import ABCMeta, abstractmethod
from _collections import deque

import lightstreamer_adapter.protocol as protocol
import lightstreamer_adapter.metadata_protocol as meta_protocol
import lightstreamer_adapter.data_protocol as data_protocol
from lightstreamer_adapter.interfaces.data import (DataProvider,
                                                   SubscribeError)
from lightstreamer_adapter.interfaces.metadata import Mode, MetadataProvider

from lightstreamer_adapter.protocol import RemotingException


__all__ = ['Server', 'DataProviderServer', 'MetadataProviderServer',
           'ExceptionHandler']


class _Worker(Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, name, tasks):
        super(_Worker, self).__init__(name=name)
        self.tasks = tasks
        self.daemon = False
        self._executing = Event()
        self.start()
        # log.debug("Started Worker %s", name)

    def run(self):
        while True:
            if self._executing.isSet():
                break
            task = self.tasks.get()
            try:
                if task == "END":
                    # print("Terminated %s", self.name)
                    break
                # print("Running from %s", self.name)
                task()  # func(*args, **kargs)
            except Exception as err:
                # print(err)
                pass
            finally:
                self.tasks.task_done()

    def shutdown(self):
        self._executing.set()


class _ThreadPool:

    def __init__(self, num_of_threads=4):
        self.pool = queue.Queue(num_of_threads)
        self.workers = []
        for i in range(0, num_of_threads):
            self.workers.append(_Worker("Worker " + str(i), self.pool))

    def submit(self, func, *args, **kargs):
        # self.pool.put((func, args, kargs))
        def task():
            func(*args, **kargs)

        self.pool.put(task)

    def join(self):
        dismissed = []
        for worker in self.workers:
            self.pool.put("END")
            dismissed.append(worker)

        for worker in dismissed:
            worker.join()

        self.pool.join()


def notify(function):
    def wrap(obj, reply):
        timestamp = int(round(time.time() * 1000))
        notification = "|".join([str(timestamp), reply])
        function(obj, notification)

    return wrap


class _Sender(object):
    """Helper class which manages the communications from the Remote Adapter to
    the ProxyAdapter, sending data over the unidirectional "replies" or
    "notifications" channels.
    sender.
    """

    STOP_WAITING_PILL = "STOP_WAITING_PILL"

    def __init__(self, sock, name, keep_alive, log):
        self.sock = sock
        self.name = name
        self.keep_alive = keep_alive
        self.log = log
        self.keep_alive_log = logging.getLogger(log.name + ".keep_alives")
        self.queue = None
        self.send_thread = None

    def start(self):
        """Starts the management of unidirectional communications from the
        Remote Adapter to the Proxy Adapter.
        """
        # Creates a queue to enqueue reading incoming replies/notifications to
        # be sent to the ProxyAdapter.
        self.queue = queue.Queue()

        # Starts new thread for dequeuing replies/notifications and then
        # sending to the ProxyAdapter.
        self.send_thread = Thread(target=self.do_run, name="Sender-Thread-{}"
                                  .format(self.name))
        self.send_thread.start()

    def send(self, ntfy):
        """Enqueues a reply or notification to be sent to the Proxy Adapter.
        """
        self.queue.put(ntfy)

    def do_run(self):
        """Target method for the Sender-Thread-XXX, started in the
        'start' method."""
        self.log.info("'%s' starting", self.name)
        while True:
            try:
                notification = None
                if self.keep_alive > 0:
                    try:
                        notification = self.queue.get(timeout=self.keep_alive)
                        self.queue.task_done()
                    except queue.Empty:
                        self.keep_alive_log.debug("Keep alive triggered...")
                else:
                    notification = self.queue.get()
                    self.queue.task_done()
                if notification == _Sender.STOP_WAITING_PILL:
                    # Request of stopping dequeuing, thread termination.
                    break

                if notification is None:
                    # Keepalive Timeout triggered.
                    notification = protocol.METHOD_KEEP_ALIVE
                    self.keep_alive_log.debug("line: %s", notification)
                else:
                    self.log.debug("line: %s", notification)

                # Sends notification over the network.
                self.sock.sendall(bytes(notification + '\r\n', 'utf-8'))
            finally:
                pass

        self.log.info("'%s' stopped", self.name)

    def quit(self):
        """Terminates the unidirectional communications with the Proxy Adapter.
        """
        # Enqueues the STOP_WAITING_PILL to notify the Sender-Thread of
        # stopping dequeuing of incoming replies/notifications.
        self.queue.put(_Sender.STOP_WAITING_PILL)
        self.queue.join()
        self.send_thread.join()


class _RequestReceiver():
    """Helper class which manages the communications with the Proxy Adpater
    counterpart.
    """

    def __init__(self, sock, server):
        self.log = logging.getLogger(("lightstreamer-adapter.requestreply"
                                      ".requests"))
        self.sock = sock
        self.server = server
        reply_sender_log = logging.getLogger(("lightstreamer-adapter."
                                              "requestreply.replies."
                                              "ReplySender"))
        self.reply_sender = _Sender(sock=sock, name=server.name,
                                    keep_alive=server.keep_alive,
                                    log=reply_sender_log)
        self.stop_request = Event()

    def start(self):
        """Starts the management of bidirectional communications with the Proxy
        Adapter: requests coming form the Proxy Adapter, and the responses
        coming from the Remote Adapters.
        """
        # Starts new thread for reading data from the socket.
        thread = Thread(target=self.do_run,
                        name="RequestReceiver-Thread-{}"
                        .format(self.server.name),
                        args=(self.sock,))
        thread.start()

        # Starts the reply sender.
        self.reply_sender.start()

    def do_run(self, sock):
        """Target method for the RequestReceiver-Thread-XXX, started in the
        'start' method."""
        self.log.info("Request receiver '%s' starting...", self.server.name)
        while not self.stop_request.is_set():
            request = None
            try:
                data = b''
                while True:
                    self.log.debug("Reading from socket...")
                    more = sock.recv(1024)
                    self.log.debug("Received %d data", len(more))
                    if not more:
                        raise EOFError('Socket connection broken')
                    data += more
                    if data.endswith(b'\n'):
                        break
                request = data.decode()
                self.log.debug("Request line: %s", request)
            except Exception as err:
                if self.stop_request.is_set():
                    self.log.debug(("Error raised because of explicitly "
                                    "closed socket, no issue"))
                    break
                # An exception has been raised, due to some issue
                # in the network communication.
                self.log.exception(("Exception while consuming data from the "
                                    "socket"))
                self.server._on_ioexception(err)
                break

            self.server._on_received_request(request)

        self.log.info("Request receiver '%s' stopped", self.server.name)

    def send_reply(self, request_id, response):
        """Sends a response to the Proxy Adapter."""
        reply = '|'.join((request_id, response))
        self.reply_sender.send(reply)

    def quit(self):
        """Terminates the communications with the Proxy Adapter.
        """
        # Issues a request to terminate the 'RequestReceiver-Thread-XXX'
        # thread.
        self.stop_request.set()

        # Issues a request to terminate the reply sender.
        self.reply_sender.quit()


class Server(metaclass=ABCMeta):
    """An abstract class meant to be extended, which represents a generic
    Remote Adapter object capable to run Remote Data or Metadata Adapter and
    connect it to the Proxy Adapter running on Lightstreamer Server.

    An instance of a Server's subclass should be provided with a suitable
    Adapter instance and with suitable initialization parameters and
    established connections, then activated through its own ``start`` and
    finally disposed through its own ``close``. Further reuse of the same
    instance is not supported.
    """

    _DEFAULT_POOL_SIZE = 4

    # Number of current instances of Server' subclasses.
    _number = 0

    def __init__(self, address, name, keep_alive, thread_pool_size):
        Server._number += 1

        # Logger actually overridden by subclasses.
        self._log = logging.getLogger("lightstreamer-adapter.server")
        self._exception_handler = None
        self._config = {}
        self._config['address'] = address
        self._config['name'] = "#{}".format(Server._number) if (name is
                                                                None) else name
        self._config['keep_alive'] = max(0, keep_alive) if (keep_alive is not
                                                            None) else 0
        pool = max(0, thread_pool_size) if thread_pool_size is not None else 0
        if pool == 0:
            try:
                self._config['thread_pool_size'] = multiprocessing.cpu_count()
            except NotImplementedError:
                self._config['thread_pool_size'] = Server._DEFAULT_POOL_SIZE
        else:
            self._config['thread_pool_size'] = pool
        self.thread_pool = None
        self._server_sock = None
        self._request_receiver = None

    @property
    def name(self):
        """The name, used for logging purposes, associated to the Server
        instance.

        :type: str
        """
        return self._config['name']

    @property
    def keep_alive(self):
        """The keepalive interval expressed in seconds (or fractions)

        :type: float
        """
        return self._config['keep_alive']

    @property
    def thread_pool_size(self):
        """The thread pool size

        :type: int
        """
        return self._config['thread_pool_size']

    def set_exception_handler(self, handler):
        """Sets the handler for error conditions occurring on the Remote
        Server. By setting the handler, it's possible to override the default
        exception handling.

        :param lightstreamer_adapter.server.ExceptionHandler handler: the
         handler for error conditions occurring on the Remote Server.
        """
        self._exception_handler = handler

    @abstractmethod
    def start(self):
        if self.keep_alive > 0:
            self._log.info("Keepalive time for %s set to %f milliseconds",
                           self.name, self.keep_alive)
        else:
            self._log.info("Keepalive for %s disabled", self.name)

        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.connect(self._config['address'])

        # Start the Thread Pool
        self.thread_pool = _ThreadPool(self._config['thread_pool_size'])

        # Setup and start the Request Receiver.
        self._request_receiver = _RequestReceiver(sock=self._server_sock,
                                                  server=self)
        self._request_receiver.start()

        # Invokes hook to notify subclass that the Request Receiver
        # has been started.
        self._on_request_receiver_started()

    def close(self):
        """Stops the management of the Remote Adapter and destroys the threads
        used by this Server. This instance can no longer be used.

        Note that this does not stop the supplied Remote Adapter, as no close
        method is available in the Remote Adapter interface. If the process is
        not terminating, then the Remote Adapter cleanup should be performed by
        accessing the supplied Adapter instance directly and calling custom
        methods.
        """
        self._server_sock.close()
        self._request_receiver.quit()
        self.thread_pool.join()

    def _on_received_request(self, request):
        try:
            parsed_request = protocol.parse_request(request)
            if parsed_request is None:
                self._log.warning("Discarding malformed request: %s", request)
                return

            request_id = parsed_request["id"]
            method = parsed_request["method"]
            data = parsed_request["test_data"]
            self._handle_request(request_id, data, method)
        except Exception as err:
            self._log.exception("Exception while handling a request")
            self._on_exception(err)

    def _send_reply(self, request_id, response):
        self._log.debug("Processing request: %s", request_id)
        self._request_receiver.send_reply(request_id, response)

    def _on_ioexception(self, ioexception):
        if self._exception_handler is not None:
            # Enable default handling in case of False
            self._log.info(("Caught exception: %s, notifying the "
                            "application..."), str(ioexception))
            if not self._exception_handler.handle_ioexception(ioexception):
                return

        self._handle_ioexception(ioexception)

    def _on_exception(self, exception):
        if self._exception_handler is not None:
            # Enable default handling in case of False
            self._log.info(("Caught exception: %s, notifying the "
                            "application..."), str(exception))
            if not self._exception_handler.handle_exception(exception):
                return

        self._handle_exception(exception)

    @abstractmethod
    def _handle_ioexception(self, ioexception):
        sys.exit(1)
        return False

    def _handle_exception(self, ioexception):
        pass

    @abstractmethod
    def _on_request_receiver_started(self):
        """Method intended to be overridden by subclasses. This method is an
        hook to notify the subclass that the Request Receiver  has been started.
        """
        pass

    @abstractmethod
    def _handle_request(self, request_id, data, method):
        """Intended to be overridden by subclasses, invoked for handling the
        received request, already splitted into the supplied parameters.
        """
        pass


class MetadataProviderServer(Server):
    """A server object which can run a Remote Metadata Adapter and connect it
    to the Proxy Adapter running on Lightstreamer Server.

    The object should be provided with a MetadataProvider instance and with
    suitable initialization parameters and established connections,
    then activated through
    :meth:`lightstreamer_adapter.server.MetadataProviderServer.start` and
    finally disposed through :meth:`lightstreamer_adapter.server.Server.close`.
    Further reuse of the same instance is not supported.

    The server will take care of sending keepalive packets on the connections
    when needed. The interval can be configured through the provided
    ``keep_alive`` parameter; by default it is 1 sec; a value of 0, negative or
    ``None`` disables the keepalives.

    By default, the invocations to the Metadata Adapter methods will be done in
    a limited thread pool with a size determined by the number of detected cpu
    cores. The size be specified through the provided ``thread_pool_size``
    parameter. A size of 1 enforces strictly sequential invocations and can be
    used if parallelization of the calls is not supported by the Metadata
    Adapter. A value of 0, negative or ``None`` also implies the default
    behaviour as stated above.

    Note that requests with an implicit ordering, like ``notify_new_session``
    and ``notify_session_closes`` for the same session, are always guaranteed
    to be and sequentialized in the right way, although they may not occur in
    the same thread.
    """
    def __init__(self, adapter, address, name=None, keep_alive=1,
                 thread_pool_size=0):
        """Creates a server with the supplied configuration parameters. The
        :meth:`lightstreamer_adapter.interfaces.metadata.MetadataProvider.initialize`
        method of the Remote Adapter will be invoked only upon a Proxy Adapter
        request.

        :param lightstreamer_adapter.interfaces.metadata.MetadataProvider \
        adapter: the Remote Metadata Adapter instance to be run.
        :param tuple address: the address of the Proxy Adapter supplied as a
         2-tuple ``(host, request_reply_port)`` where:

         * host: a string representing the hostname or the IP address
         * request_reply_port: an int representing the request/reply port
        :raises TypeError: if the supplied Remote Adapter is not an instance of
         a subclass of
         :class:`lightstreamer_adapter.interfaces.metadata.MetadataProvider`.
        """
        super(MetadataProviderServer, self).__init__(address, name, keep_alive,
                                                     thread_pool_size)
        if not isinstance(adapter, MetadataProvider):
            raise TypeError(("The provided adapter is not a subclass of "
                             "lightstreamer_adapter.interfaces."
                             "MetadatadataProvider"))
        self._log = logging.getLogger(("lightstreamer-adapter.server."
                                       "MetadataProviderServer"))
        self._config_file = None
        self._params = None
        self._adapter = adapter
        self.init_expected = True

    def _on_request_receiver_started(self):
        """Method intended to be overridden by subclasses. This method is an
        hook to notify the subclass that the Request Receiver has been started.
        This class has a void implementation.
        """
        pass

    def _handle_request(self, request_id, data, method):
        init_request = method == meta_protocol.Method.MPI.name
        if init_request and not self.init_expected:
            raise RemotingException("Unexpected late {} request"
                                    .format(str(meta_protocol.Method.MPI)))
        elif not init_request and self.init_expected:
            raise RemotingException(("Unexpected request {} while waiting for "
                                     "a {} request")
                                    .format(method, meta_protocol.Method.MPI))
        if init_request:
            self.init_expected = False
            res = self._on_mpi(data)
            self._send_reply(request_id, res)
            return

        # Builds the name of the method do be invoked, starting from the
        # protocol method name.
        on_method = "_on_" + method.lower()

        # Invokes the method and gets the returned asynchronous function to be
        # executed through the thread pool.
        try:
            async_func = getattr(self, on_method)(data)
        except AttributeError as err:
            self._log.warning("Discarding unknown request: %s", method);
            return

        # Task wrapping execution of the returned async_func and
        # successive reply.
        def execute_and_reply():
            try:
                response = async_func()
                self._send_reply(request_id, response)
            except Exception as err:
                self._on_exception(err)

        self.thread_pool.submit(execute_and_reply)

    def _on_mpi(self, data):
        parsed_data = meta_protocol.read_init(data)
        try:
            if self._params:
                init_params = self._params.copy()
                parsed_data.update(init_params)
            self._adapter.initialize(parsed_data, self._config_file)
        except Exception as err:
            res = meta_protocol.write_init(err)
        else:
            res = meta_protocol.write_init()
        return res

    def _on_nus(self, data):
        parsed_data = meta_protocol.read_notify_user(data)
        user = parsed_data["user"]
        password = parsed_data["password"]
        http_headers = parsed_data["httpHeaders"]

        def execute():
            try:
                self._adapter.notify_user(user, password, http_headers)
                max_bandwidth = self._adapter.get_allowed_max_bandwidth(user)
                wants_tb_ntf = self._adapter.wants_tables_notification(user)
            except Exception as err:
                res = meta_protocol.write_notiy_user(meta_protocol.Method.NUS,
                                                     exception=err)
            else:
                res = meta_protocol.write_notiy_user(meta_protocol.Method.NUS,
                                                     max_bandwidth,
                                                     wants_tb_ntf)
            return res
        return execute

    def _on_nua(self, data):
        parsed_data = meta_protocol.read_notify_user_auth(data)
        user = parsed_data["user"]
        http_headers = parsed_data["httpHeaders"]
        password = parsed_data["password"]
        client_principal = parsed_data["clientPrincipal"]

        def execute():
            try:
                self._adapter.notify_user_with_principal(user, password,
                                                         http_headers,
                                                         client_principal)
                max_bandwidth = self._adapter.get_allowed_max_bandwidth(user)
                wants_tb_notify = self._adapter.wants_tables_notification(user)
            except Exception as err:
                res = meta_protocol.write_notiy_user(meta_protocol.Method.NUA,
                                                     exception=err)
            else:
                res = meta_protocol.write_notiy_user(meta_protocol.Method.NUA,
                                                     max_bandwidth,
                                                     wants_tb_notify)
            return res
        return execute

    def _on_nns(self, data):
        parsed_data = meta_protocol.read_notify_new_session(data)
        session_id = parsed_data["session_id"]
        user = parsed_data["user"]
        client_context = parsed_data["clientContext"]

        def execute():
            try:
                self._adapter.notify_new_session(user,
                                                 session_id,
                                                 client_context)
                res = meta_protocol.write_notify_new_session()
            except Exception as err:
                res = meta_protocol.write_notify_new_session(err)
            return res
        return execute

    def _on_nsc(self, data):
        session_id = meta_protocol.read_notifiy_session_close(data)

        def execute():
            try:
                self._adapter.notify_session_close(session_id)
            except Exception as err:
                res = meta_protocol.write_notify_session_close(err)
            else:
                res = meta_protocol.write_notify_session_close()
            return res
        return execute

    def _on_gis(self, data):
        parsed_data = meta_protocol.read_get_items(data)
        session_id = parsed_data["session_id"]
        user = parsed_data["user"]
        group = parsed_data["group"]

        def execute():
            try:
                items = self._adapter.get_items(user, session_id, group)
                if not items:
                    self._log.warning(("None or empty field list from "
                                       "get_items for group '%s'"), group)
                res = meta_protocol.write_get_items(items)
            except Exception as err:
                res = meta_protocol.write_get_items(exception=err)
            return res
        return execute

    def _on_gsc(self, data):
        parsed_data = meta_protocol.read_get_schema(data)
        session_id = parsed_data["session_id"]
        group = parsed_data["group"]
        user = parsed_data["user"]
        schema = parsed_data["schema"]

        def execute():
            try:
                fields = self._adapter.get_schema(user, session_id, group,
                                                  schema)
                if not fields:
                    self._log.warning(("None or empty field list from "
                                       "get_schema for schema '%s' in group "
                                       "'%s'"), schema, group)
                res = meta_protocol.write_get_schema(fields)
            except Exception as err:
                res = meta_protocol.write_get_schema(exception=err)

            return res
        return execute

    def _on_git(self, data):
        parsed_data = meta_protocol.read_get_item_data(data)

        def execute():
            try:
                items = [{"item": item,
                          "allowedModeList":
                          [mode for mode in list(Mode)
                           if self._adapter.mode_may_be_allowed(item, mode)],
                          "distinctSnapshotLength":
                          self._adapter.get_distinct_snapshot_length(item),
                          "minSourceFrequency":
                          self._adapter.get_min_source_frequency(item)}
                         for item in parsed_data]
            except Exception as err:
                res = meta_protocol.write_get_item_data(exception=err)
            else:
                res = meta_protocol.write_get_item_data(items)
            return res
        return execute

    def _on_gui(self, data):
        parsed_data = meta_protocol.read_get_user_item_data(data)
        user = parsed_data["user"]
        user_items = parsed_data["items"]

        def execute():
            try:
                items = [{"item": item,
                          "allowedModeList":
                          [mode for mode in list(Mode)
                           if self._adapter.ismode_allowed(user, item, mode)],
                          "allowedBufferSize":
                          self._adapter.get_allowed_buffer_size(user, item),
                          "allowedMaxFrequency":
                          self._adapter.get_allowed_max_item_frequency(user,
                                                                       item)}
                         for item in user_items]
            except Exception as err:
                res = meta_protocol.write_get_user_item_data(exception=err)
            else:
                res = meta_protocol.write_get_user_item_data(items)
            return res
        return execute

    def _on_num(self, data):
        parsed_data = meta_protocol.read_notify_user_message(data)
        session_id = parsed_data["session_id"]
        message = parsed_data["message"]
        user = parsed_data["user"]

        def execute():
            try:
                self._adapter.notify_user_message(user, session_id, message)
            except Exception as err:
                res = meta_protocol.write_notify_user_message(err)
            else:
                res = meta_protocol.write_notify_user_message()
            return res
        return execute

    def _on_nnt(self, data):
        parsed_data = meta_protocol.read_notify_new_tables(data)
        session_id = parsed_data["session_id"]
        user = parsed_data["user"]
        table_infos = parsed_data["tableInfos"]

        def execute():
            try:
                self._adapter.notify_new_tables(user, session_id, table_infos)
            except Exception as err:
                res = meta_protocol.write_notify_new_tables_data(err)
            else:
                res = meta_protocol.write_notify_new_tables_data()
            return res
        return execute

    def _on_ntc(self, data):
        data = meta_protocol.read_notify_tables_close(data)
        table_infos = data["tableInfos"]
        session_id = data["session_id"]

        def execute():
            try:
                self._adapter.notify_tables_close(session_id, table_infos)
            except Exception as err:
                res = meta_protocol.write_notify_tables_close(err)
            else:
                res = meta_protocol.write_notify_tables_close()
            return res
        return execute

    def _on_mda(self, data):
        parsed_data = meta_protocol.read_notify_device_access(data)
        mpn_device_info = parsed_data["mpnDeviceInfo"]
        user = parsed_data["user"]

        def execute():
            try:

                self._adapter.notify_mpn_device_access(user, mpn_device_info)
            except Exception as err:
                res = meta_protocol.write_notify_device_acces(err)
            else:
                res = meta_protocol.write_notify_device_acces()
            return res
        return execute

    def _on_msa(self, data):
        parsed_data = meta_protocol.read_subscription_activation(data)
        session_id = parsed_data["session_id"]
        table = parsed_data["table"]
        subscription = parsed_data["subscription"]
        user = parsed_data["user"]

        def execute():
            try:
                self._adapter.notify_mpn_subscription_activation(user,
                                                                 session_id,
                                                                 table,
                                                                 subscription)
            except Exception as err:
                res = meta_protocol.write_subscription_activation(err)
            else:
                res = meta_protocol.write_subscription_activation()
            return res
        return execute

    def _on_mdc(self, data):
        parsed_data = meta_protocol.read_device_token_change(data)
        mpn_device_info = parsed_data["mpnDeviceInfo"]
        user = parsed_data["user"]
        new_device_toksn = parsed_data["newDeviceToken"]

        def execute():
            try:
                self._adapter.notify_mpn_device_token_change(user,
                                                             mpn_device_info,
                                                             new_device_toksn)
            except Exception as err:
                res = meta_protocol.write_device_token_change(err)
            else:
                res = meta_protocol.write_device_token_change()
            return res
        return execute

    def _handle_ioexception(self, ioexception):
        self._log.fatal(("Exception caught while reading/writing from/to "
                         "network: <%s>, aborting..."), str(ioexception))
        super(MetadataProviderServer, self)._handle_ioexception(ioexception)

    def _handle_exception(self, exception):
        self._log.error("Caught exception: %s", str(exception))
        return False

    @property
    def adapter_config(self):
        """The pathname of an optional configuration file for the Remote
        Metadata Adapter, to be passed to the
        :meth:`lightstreamer_adapter.interfaces.metadata.MetadataProvider.initialize`
        method.

        :Getter: Returns the pathname of the optional configuration file
        :Setter: Sets the pathname of the optional configuration file
        :type: str
        """
        return self._config_file

    @adapter_config.setter
    def adapter_config(self, value):
        self._config_file = value

    @property
    def adapter_params(self):
        """A dictionary object to be passed to the
        :meth:`lightstreamer_adapter.interfaces.metadata.MetadataProvider.initialize`
        method of the Remote Metadata Adapter, to supply optional parameters.

        :Getter: Returns the dictionary object of optional parameters
        :Setter: Sets the dictionary object of optional parameters
        :type: dict
        """
        return self._params

    @adapter_params.setter
    def adapter_params(self, value):
        self._params = value

    def start(self):
        """Starts the Remote Metadata Adapter. A connection to the Proxy
        Adapter is performed (as soon as one is available). Then, requests
        issued by the Proxy Adapter are received and forwarded to the Remote
        Adapter.

        :raises \
        lightstreamer_adapter.interfaces.metadata.MetadataProviderError: If an
         error occurred in the initialization phase. The adapter was not
         started.
        :raises Exception: if an error occurred in the initialization phase.
         The adapter was not started.
        """
        self._log.info("Managing Metadata Adapter %s a thread pool size of %s",
                       self.name, self.thread_pool_size)
        super(MetadataProviderServer, self).start()


class _ItemTask():

    def __init__(self, request_id, issubscribe, do_task, do_late_task):
        self._request_id = request_id
        self._issubscribe = issubscribe
        self._do_task = do_task
        self._do_late_task = do_late_task

    def do_task(self):
        return self._do_task()

    def do_late_task(self):
        self._do_late_task()

    @property
    def code(self):
        return self._request_id

    @property
    def issubscribe(self):
        return self._issubscribe


class _ItemManager():

    def __init__(self, item_name, server, thread_pool):
        self.item_name = item_name
        self.tasks = deque()
        self.thread_pool = thread_pool
        self._code = None
        self._isrunning = False
        self._lock = Lock()
        self.queued = 0
        self._last_subscribe_outcome = False
        self._server = server

    @property
    def code(self):
        return self._code

    def add_task(self, task):
        with self._lock:
            self.tasks.append(task)
            if not self._isrunning:
                self._isrunning = True
                self.thread_pool.submit(self.deque)

    def deque(self):
        dequeued = 0
        last_subscribe_outcome = True
        while True:
            with self._lock:
                if dequeued == 0:
                    last_subscribe_outcome = self._last_subscribe_outcome
                if len(self.tasks) == 0:
                    self._isrunning = False
                    self._last_subscribe_outcome = last_subscribe_outcome
                    break
                # Deque next _ItemTask
                item_task = self.tasks.popleft()
                islast = len(self.tasks) == 0
                dequeued += 1
            try:
                if item_task.issubscribe:
                    # Task is a Subscription
                    if not islast:
                        item_task.do_late_task()
                        last_subscribe_outcome = False
                    else:
                        with self._server.items_lock:
                            self._code = item_task.code
                        last_subscribe_outcome = item_task.do_task()
                else:
                    # Task is an Unsubscription
                    if last_subscribe_outcome:
                        item_task.do_task()
                    else:
                        item_task.do_late_task()
                    with self._server.items_lock:
                        self._code = None
            except Exception:
                # self._log.exception("Caught an exception")
                pass

        with self._server.items_lock:
            self.queued -= dequeued
            if not self._code and self.queued == 0:
                item_manager = self._server.active_items.get(self.item_name)
                if not item_manager:
                    pass
                elif item_manager != self:
                    pass
                else:
                    del self._server.active_items[self.item_name]


class DataProviderServer(Server):
    """A server object which can run a Remote Data Adapter and connect
    it to the Proxy Adapter running on Lightstreamer Server.

    The object should be provided with a DataProvider instance and with
    suitable initialization parameters and established connections, then
    activated through
    :meth:`lightstreamer_adapter.server.DataProviderServer.start()`
    and finally disposed through
    :meth:`lightstreamer_adapter.server.Server.close()`.
    Further reuse of the same instance is not supported.

    The server will take care of sending keepalive packets on the connections
    when needed. The interval can be configured through the provided
    ``keep_alive`` parameter; by default it is 1 sec; a value of 0, negative or
    ``None`` disables the keepalives.

    By default, the invocations to the Data Adapter methods will be done in
    a limited thread pool with a size determined by the number of detected cpu
    cores. The size size can be specified through the provided
    ``thread_pool_size`` parameter. A size of 1 enforces strictly sequential
    invocations and can be used if parallelization of the calls is not
    supported by the Metadata Adapter. A value of 0, negative or ``None`` also
    implies the default behaviour as stated above.

    Note that Subscribe and Unsubscribe invocations for the same item are
    always guaranteed to be sequentialized in the right way, although they may
    not occur in the same thread.
    """

    def __init__(self, adapter, address, name=None, keep_alive=1,
                 thread_pool_size=0):
        """Creates a server with the supplied configuration parameters. The
        initialize method of the Remote Adapter will be invoked only upon a
        Proxy Adapter request.

        :param lightstreamer_adapter.interfaces.data.DataProvider adapter: The
         Remote Adapter instance to be run.
        :param tuple address: the address of the Proxy Adapter supplied as a
         3-tuple ``(host, request_reply_port, notify_port)`` where:

         * host: a string representing the hostname or the IP address
         * request_reply_port: an int representing the request/reply port
         * notify_port: an int representing the notify port
        :param str name: the name associated to the Server instance.
        :param float keep_alive: the keepalive interval expressed in seconds
         (or fractions)
        :param int thread_pool_size: the thread pool size
        :raises TypeError: if the supplied Remote Adapter is not an instance of
         a subclass of
         :class:`lightstreamer_adapter.interfaces.data.DataProvider`.
        """
        super(DataProviderServer, self).__init__((address[0], address[1]),
                                                 name,
                                                 keep_alive,
                                                 thread_pool_size)

        if not isinstance(adapter, DataProvider):
            raise TypeError(("The provided adapter is not a subclass of "
                             "lightstreamer_adapter.interfaces.DataProvider"))
        self._log = logging.getLogger(("lightstreamer-adapter.server."
                                       "dataproviderserver"))
        self._config_file = None
        self._params = None
        self._adapter = adapter
        self.init_expected = True
        self.active_items = {}
        self.items_lock = RLock()
        self._notify_sender = None
        self.notify_address = (address[0], address[2])
        self._notify_sock = None

    def _handle_request(self, request_id, data, method):
        init_request = method == data_protocol.Method.DPI.name
        if init_request and not self.init_expected:
            raise RemotingException("Unexpected late {} request"
                                    .format(str(data_protocol.Method.DPI)))
        elif not init_request and self.init_expected:
            raise RemotingException(("Unexpected request {} while waiting for "
                                     "a {} request")
                                    .format(method, data_protocol.Method.DPI))
        if init_request:
            self.init_expected = False
            res = self._on_dpi(data)
            self._send_reply(request_id, res)
        elif method == "SUB":
            self._on_sub(request_id, data)
        elif method == "USB":
            self._on_usb(request_id, data)
        else:
            self._log.warning("Discarding unknown request: %s", method)

    @property
    def adapter_config(self):
        """The pathname of an optional configuration file for the Remote
        Data Adapter, to be passed to the
        :meth:`lightstreamer_adapter.interfaces.data.DataProvider.initialize`
        method.

        :Getter: Returns the pathname of the optional configuration file
        :Setter: Sets the pathname of the optional configuration file
        :type: str
        """
        return self._config_file

    @adapter_config.setter
    def adapter_config(self, value):
        self._config_file = value

    @property
    def adapter_params(self):
        """A dictionary object to be passed to the
        :meth:`lightstreamer_adapter.interfaces.data.DataProvider.initialize`
        method of the Remote Data Adapter, to supply optional parameters.

        :Getter: Returns the dictionary object of optional parameters
        :Setter: Sets the dictionary object of optional parameters
        :type: dict
        """
        return self._params

    @adapter_params.setter
    def adapter_params(self, value):
        self._params = value

    def _on_request_receiver_started(self):
        """Method intended to be overridden by subclasses. This method is an
        hook to notify the subclass that the Request Receiver has been started.
        This class creates an additional socket to enable the unidirectional
        communication from the Remote Data Adapter to the Proxy Adapter, in
        order to send data over the notification channel.
        """
        self._notify_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._notify_sock.connect(self.notify_address)
        notify_sender_log = logging.getLogger(("lightstreamer-adapter."
                                               "requestreply.notifications."
                                               "NotifySender"))
        self._notify_sender = _Sender(sock=self._notify_sock, name=self.name,
                                      keep_alive=self.keep_alive,
                                      log=notify_sender_log)
        self._notify_sender.start()

    def _get_active_item(self, item_name):
        with self.items_lock:
            if item_name in self.active_items:
                item_manager = self.active_items[item_name]
                return item_manager.code

    def _on_dpi(self, data):
        parsed_data = data_protocol.read_init(data)
        try:
            if self._params:
                init_params = self._params.copy()
                parsed_data.update(init_params)

            self._adapter.initialize(parsed_data, self._config_file)
            self._adapter.set_listener(self)
        except Exception as err:
            res = data_protocol.write_init(err)
        else:
            res = data_protocol.write_init()
        return res

    def _on_sub(self, request_id, data):
        item = data_protocol.read_sub(data)

        def do_task():
            success = False
            try:
                snapshot_available = self._adapter.issnapshot_available(item)
                if snapshot_available:
                    self.end_of_snapshot(item)
                self._adapter.subscribe(item)
                success = True
            except Exception as err:
                res = data_protocol.write_sub(err)
            else:
                res = data_protocol.write_sub()
            self._send_reply(request_id, res)
            return success

        def do_late_task():
            self._log.info("Skipping request: %s", request_id)
            subscribe_err = SubscribeError("Subscribe request come too late")
            res = data_protocol.write_sub(subscribe_err)
            return res

        sub_task = _ItemTask(request_id, True, do_task, do_late_task)
        with self.items_lock:
            if item not in self.active_items:
                self.active_items[item] = _ItemManager(item, self,
                                                       self.thread_pool)
            item_manager = self.active_items[item]
            item_manager.queued += 1
        item_manager.add_task(sub_task)

    def _on_usb(self, request_id, data):
        item_name = data_protocol.read_usub(data)

        def do_task():
            success = False
            try:
                self._adapter.unsubscribe(item_name)
                success = True
            except Exception as err:
                res = data_protocol.write_unsub(err)
            else:
                res = data_protocol.write_unsub()
            self._send_reply(request_id, res)
            return success

        def do_late_task():
            self._log.info("Skipping request: %s", request_id)
            res = data_protocol.write_unsub()
            self._send_reply(request_id, res)

        unsub_task = _ItemTask(request_id, False, do_task, do_late_task)
        with self.items_lock:
            if item_name not in self.active_items:
                self._log.error("Task list expected for item %s", item_name)
                return
            item_manager = self.active_items[item_name]
            item_manager.queued += 1
        item_manager.add_task(unsub_task)

    @notify
    def _send_notify(self, ntfy):
        self._notify_sender.send(ntfy)

    def update(self, item_name, events_map, issnapshot):
        request_id = self._get_active_item(item_name)
        if request_id:
            try:
                res = data_protocol.write_update_map(item_name, request_id,
                                                     issnapshot, events_map)
                self._send_notify(res)
            except protocol.RemotingException as err:
                self._on_exception(err)
        else:
            self._log.warning("Unexpected update for item %s", item_name)

    def end_of_snapshot(self, item_name):
        request_id = self._get_active_item(item_name)
        if request_id:
            try:
                res = data_protocol.write_eos(item_name, request_id)
                self._send_notify(res)
            except protocol.RemotingException as err:
                self._on_exception(err)
        else:
            self._log.warning("Unexpected end_of_snapshot notify for item %s",
                              item_name)

    def clear_snapshot(self, item_name):
        request_id = self._get_active_item(item_name)
        if request_id:
            try:
                res = data_protocol.write_cls(item_name, request_id)
                self._send_notify(res)
            except protocol.RemotingException as err:
                self._on_exception(err)
        else:
            self._log.warning("Unexpected clear_snapshot for item %s",
                              item_name)

    def failure(self, exception):
        try:
            res = data_protocol.write_failure(exception)
            self._send_notify(res)
        except protocol.RemotingException as err:
            self._on_exception(err)

    def _handle_ioexception(self, ioexception):
        self._log.fatal(("Exception caught while reading/writing from/to "
                         "network: <%s>, aborting..."), str(ioexception))
        super(DataProviderServer, self)._handle_ioexception(ioexception)

    def _handle_exception(self, exception):
        self._log.error("Caught exception: %s, trying to notify a failure...",
                        str(exception))
        try:
            res = data_protocol.write_failure(exception)
            self._send_notify(res)
        except Exception:
            self._log.exception(("Caught second-level exception while trying "
                                 "to notify a first-level exception"))

        return False

    def start(self):
        """Starts the Remote Data Adapter. A connection to the Proxy Adapter is
        performed (as soon as one is available). Then, requests issued by the
        Proxy Adapter are received and forwarded to the Remote Adapter.

        :raises lightstreamer_adapter.interfaces.data.DataProviderError: If an
         error occurred in the initialization phase. The adapter was not
         started.
        :raises Exception: if an error occurred in the initialization phase.
         The adapter was not started.
        """
        super(DataProviderServer, self).start()

    def close(self):
        super(DataProviderServer, self).close()
        self._notify_sender.quit()
        self._log.info("Stopped")


class ExceptionHandler(metaclass=ABCMeta):
    """Abstract class to to be implemented in order to provide the Server
    instance with a custom handler for error conditions occurring on the Remote
    Server.
    """
    def __init__(self):
        pass

    def handle_ioexception(self, exception):
        """Called by the Remote Server upon a read or write operation failure.
        This may mean that the connection to the Server is lost; in any way,
        after this error, the correct Lightstreamer Server and Remote Server
        operation is compromised. This can be the signal of a normal Server
        termination. If this is not the case, then Lightstreamer Server should
        be restarted and the Remote Server should be reinitialized (i.e. the
        process should be restarted or a new Server class instance should be
        used).

        The default handling closes the Remote Server. This also ensures that
        the Proxy Adapter causes the closure of Lightstreamer Server.

        :param Exception exception: An Exception showing the cause of the
         problem.
        :return bool: ``True`` to enable the default handling, false to
         suppress it.
        """
        pass

    def handle_exception(self, exception):
        """Called by the Remote Server upon an unexpected error. After this
        error, the correct Lightstreamer Server and Remote Server operation
        might be compromised. If this is the case, then Lightstreamer Server
        should be restarted and the Remote Server should be reinitialized (i.e.
        the process should be restarted or a new Server class instance should
        be used).

        The default handling, in case of a Remote Data Adapter, issues an
        asynchronous failure notification to the Proxy Adapter; this causes the
        closure of Lightstreamer Server, which, in turn, causes the
        communication channel to be closed. In case of a Remote Metadata
        Adapter, the default handling ignores the notification; however, as a
        consequence of the Remote Protocol being broken, the Proxy Adapter may
        return exceptions against one or more specific requests by
        Lightstreamer Kernel.

        :param Exception exception: An Exception showing the cause of the
         problem.
        :return: ``True`` to enable the default handling, false to
         suppress it.
        :rtype: bool
        """
