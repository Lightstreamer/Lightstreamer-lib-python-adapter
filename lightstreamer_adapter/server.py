"""
Core module of the Lightstreamer SDK for Python Adapters, containing all
classes (public and private), needed to configure and start the Remote
Adapters.
"""
import socket
import queue
import logging
import time
import os
from multiprocessing import cpu_count
from threading import Thread, Event
from concurrent.futures.thread import ThreadPoolExecutor
from abc import ABCMeta, abstractmethod

import lightstreamer_adapter.protocol as protocol
import lightstreamer_adapter.metadata_protocol as meta_protocol
import lightstreamer_adapter.data_protocol as data_protocol
from lightstreamer_adapter.interfaces.data import (DataProvider,
                                                   SubscribeError,
                                                   DataProviderError)
from lightstreamer_adapter.interfaces.metadata import (Mode,
                                                       MetadataProvider,
                                                       MetadataProviderError)
from lightstreamer_adapter.protocol import RemotingException
from lightstreamer_adapter.subscription import SubscriptionManager, ItemTask
from . import DATA_PROVIDER_LOGGER as DATA_LOGGER
from . import METADATA_PROVIDER_LOGGER as METADATA_LOGGER

__all__ = ['Server', 'DataProviderServer', 'MetadataProviderServer',
           'ExceptionHandler']


def notify(function):
    """Decorator function which add timestamp information to each notification
    sent to the Proxy Adapter.
    """
    def wrap(obj, notification):
        """"Add the timestamp to the supplied notification, in the following
        format:

        <timestamp>|<notification>.
        """
        timestamp = int(round(time.time() * 1000))
        notification = "|".join([str(timestamp), notification])
        function(obj, notification)

    return wrap


class _Sender(object):
    """Helper class which manages the communications from the Remote Adapter to
    the ProxyAdapter, sending data over the "request/replies" or
    "notifications" channels.
    """
    def __init__(self, sock, server, log):
        self._sock = sock
        self._server = server
        self._log = log
        self._keep_alive_log = logging.getLogger(log.name + ".keep_alives")
        self._send_queue = None
        self._send_thread = None

    def start(self):
        """Starts the management of communications from the Remote Adapter to
        the Proxy Adapter.
        """
        # Creates a queue to append incoming replies/notifications to be sent
        # to the ProxyAdapter.
        self._send_queue = queue.Queue()

        # Starts new thread for dequeuing replies/notifications and then
        # sending to the ProxyAdapter.
        self._send_thread = Thread(target=self._do_run, name="Sender-Thread-{}"
                                   .format(self._server.name))
        self._send_thread.start()

    def send(self, notification):
        """Enqueues a reply or notification to be sent to the Proxy Adapter."""
        self._log.debug("Enqueing line: %s", notification)
        self._send_queue.put(notification)

    def direct_send(self, notification):
        try:
            current_log = self._log
            # Send to_send over the network.
            current_log.debug("Sending line: %s", notification)
            self._sock.sendall(bytes(notification + '\r\n', 'utf-8'))
        except OSError as err:
            self._server.on_ioexception(err)

    def _do_run(self):
        """Target method for the Sender-Thread-XXX, started in the start'
        method."""
        self._log.info("'%s' starting", self._server.name)
        keep_alive = self._server.keep_alive
        while True:
            try:
                to_send = None
                current_log = self._log
                current_log.debug("Waiting for a line to send...")
                if keep_alive > 0:
                    try:
                        to_send = self._send_queue.get(timeout=keep_alive)
                    except queue.Empty:
                        # Keepalive Timeout triggered.
                        to_send = protocol.METHOD_KEEP_ALIVE
                        current_log = self._keep_alive_log
                else:
                    to_send = self._send_queue.get()

                if to_send is None:
                    # Request of stopping dequeuing, thread termination.
                    break
                # Send to_send over the network.
                current_log.debug("Sending line: %s", to_send)
                self._sock.sendall(bytes(to_send + '\r\n', 'utf-8'))
            except OSError as err:
                self._server.on_ioexception(err)
                break
        self._log.info("'%s' stopped", self._server.name)

    def quit(self):
        """Terminates the communications with the Proxy Adapter."""
        # Enqueues a None item to notify the Sender-Thread-XXX of stopping
        # dequeuing of incoming replies/notifications.
        self._send_queue.put(None)
        self._send_thread.join()


class _RequestReceiver():
    """Helper class which manages the bi-directional communications with the
    Proxy Adpater counterpart over the "request/replies" channel.
    """

    def __init__(self, sock, server):
        self._log = logging.getLogger(("lightstreamer-adapter.requestreply"
                                       ".requests"))
        self._sock = sock
        self._server = server
        reply_sender_log = logging.getLogger(("lightstreamer-adapter."
                                              "requestreply.replies."
                                              "ReplySender"))
        self._reply_sender = _Sender(sock=sock, server=self._server,
                                     log=reply_sender_log)
        self._stop_request = Event()

    def start(self):
        """Starts the management of bidirectional communications with the Proxy
        Adapter: requests coming form the Proxy Adapter, and the responses
        coming from the Remote Adapters.
        """
        # Starts new thread for reading data from the socket.
        thread = Thread(target=self._do_run,
                        name="RequestReceiver-Thread-{}"
                        .format(self._server.name),
                        args=(self._sock,))
        thread.start()

        # Starts the reply sender.
        self._reply_sender.start()

    def _do_run(self, sock):
        """Target method for the RequestReceiver-Thread-XXX, started in the
        'start' method.
        """
        self._log.info("Request receiver '%s' starting...", self._server.name)
        while not self._stop_request.is_set():
            request = None
            try:
                data = b''
                while True:
                    self._log.debug("Reading from socket...")
                    more = sock.recv(1024)
                    self._log.debug("Received %d bytes of data", len(more))
                    if not more:
                        raise EOFError('Socket connection broken')
                    data += more
                    if data.endswith(b'\n'):
                        break
                request = data.decode()
                self._log.debug("Request line: %s", request)
            except (OSError, EOFError) as err:
                if self._stop_request.is_set():
                    self._log.debug(("Error raised because of explicitly "
                                     "closed socket, no issue"))
                    break
                # An exception has been raised, due to some issue in the
                # network communication.
                self._server.on_ioexception(err)
                break

            self._server.on_received_request(request)

        self._log.info("Request receiver '%s' stopped", self._server.name)

    def send_reply(self, request_id, response):
        """Sends a reply to the Proxy Adapter.
        """
        reply = '|'.join((request_id, response))
        self._reply_sender.send(reply)

    def quit(self):
        """Terminates the communications with the Proxy Adapter.
        """
        # Issues a request to terminate the 'RequestReceiver-Thread-XXX'
        # thread.
        self._stop_request.set()

        # Issues a request to terminate the associated reply sender.
        self._reply_sender.quit()


class Server(metaclass=ABCMeta):
    """An abstract class meant to be extended, which represents a generic
    Remote Adapter object capable to run Remote Data or Metadata Adapter and
    connect it to the Proxy Adapter running on Lightstreamer Server.

    An instance of a Server's subclass should be provided with a suitable
    Adapter instance and with suitable initialization parameters and
    established connections, then activated through its own :meth:`start` and
    finally disposed through its own :meth:`close`. Further reuse of the same
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
                self._config['thread_pool_size'] = cpu_count()
            except NotImplementedError:
                self._config['thread_pool_size'] = Server._DEFAULT_POOL_SIZE
        else:
            self._config['thread_pool_size'] = pool

        self._executor = ThreadPoolExecutor(self._config['thread_pool_size'])
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
        """Starts the Remote Adapter. A connection to the Proxy Adapter is
        performed (as soon as one is available). Then, requests issued by
        the Proxy Adapter are received and forwarded to the Remote Adapter.
        """
        if self.keep_alive > 0:
            self._log.info("Keepalive time for %s set to %f milliseconds",
                           self.name, self.keep_alive)
        else:
            self._log.info("Keepalive for %s disabled", self.name)

        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.connect(self._config['address'])

        # Creates and starts the Request Receiver.
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
        self._request_receiver.quit()
        self._executor.shutdown()
        self._server_sock.close()

    def on_received_request(self, request):
        """Invoked when the RequestReciver gets a new request coming from the
        Proxy Adapter.

        This method takes the responsibility to proceed with a first
        coarse-grained parsing, to identify the three main components of the
        packet structure, as follows:

        <ID>|<method>|<data>
         |      |       |
         |      |     The arguments to be passed to the method
         |      |
         |   The method to invoke on the Remote Adapter
         |
        The Request Id

        Once parsed, the request is then dispatched to the subclass for later
        management.
        """
        try:
            parsed_request = protocol.parse_request(request)
            if parsed_request is None:
                self._log.warning("Discarding malformed request: %s", request)
                return

            request_id = parsed_request["id"]
            method_name = parsed_request["method"]
            data = parsed_request["data"]
            self._handle_request(request_id, data, method_name)
        except RemotingException as err:
            self.on_exception(err)

    def _send_reply(self, request_id, response):
        self._log.debug("Sending reply for request: %s", request_id)
        self._request_receiver.send_reply(request_id, response)

    def on_ioexception(self, ioexception):
        """Called by the Remote Server upon a read or write operation failure.

        See documentation from the ExceptionHandler.handle_io exception method
        for further details.
        """
        if self._exception_handler is not None:
            self._log.info(("Caught exception: %s, notifying the "
                            "application..."), str(ioexception))
            # Enable default handling in case the exception handler
            # returns False.
            if not self._exception_handler.handle_ioexception(ioexception):
                return

        self._handle_ioexception(ioexception)

    def on_exception(self, exception):
        """Called by the Remote Server upon an unexpected error.

        See documentation from the ExceptionHandler.handle_exception method for
        further details.
        """
        if self._exception_handler is not None:
            self._log.info(("Caught exception: %s, notifying the "
                            "application..."), str(exception))
            # Enable default handling in case the exception handler
            # returns False.
            if not self._exception_handler.handle_exception(exception):
                return

        self._handle_exception(exception)

    @abstractmethod
    def _handle_ioexception(self, ioexception):
        os._exit(1)
        return False

    def _handle_exception(self, ioexception):
        pass

    @abstractmethod
    def _on_request_receiver_started(self):
        """Hook method to notify the subclass that the Request Receiver has
        been started.

        This method is intended to be overridden by subclasses.
        """
        pass

    @abstractmethod
    def _handle_request(self, request_id, data, method_name):
        """Intended to be overridden by subclasses, invoked for handling the
        received request, already splitted into the supplied parameters.
        """
        pass


class MetadataProviderServer(Server):
    """A server object which can run a Remote Metadata Adapter and connect it
    to the Proxy Adapter running on Lightstreamer Server.

    The object should be provided with a MetadataProvider instance and with
    suitable initialization parameters and established connections,
    then activated through :meth:`MetadataProviderServer.start` and finally
    disposed through :meth:`Server.close`.
    Further reuse of the same instance is not supported.

    The server will take care of sending keepalive packets on the connections
    when needed. The interval can be configured through the provided
    ``keep_alive`` parameter; by default it is 1 sec; a value of 0, negative or
    ``None`` disables the keepalives.

    By default, the invocations to the Metadata Adapter methods will be done in
    a limited thread pool with a size determined by the number of detected cpu
    cores. The size can be specified through the provided ``thread_pool_size``
    parameter. A size of 1 enforces strictly sequential invocations and can be
    used if parallelization of the calls is not supported by the Metadata
    Adapter. A value of 0, negative or ``None`` also implies the default
    behaviour as stated above.

    Note that requests with an implicit ordering, like
    :meth:`lightstreamer_adapter.interfaces.metadata.MetadataProvider.notify_new_session`
    and
    :meth:`lightstreamer_adapter.interfaces.metadata.MetadataProvider.notify_session_close`
    for the same session, are always guaranteed to be and sequentialized in the
    right way, although they may not occur in the same thread.
    """
    def __init__(self, adapter, address, name=None, keep_alive=1,
                 thread_pool_size=0):
        """Creates a server with the supplied configuration parameters. The
        :meth:`lightstreamer_adapter.interfaces.metadata.MetadataProvider.initialize`
        method will be invoked only upon a Proxy Adapter request.

        :param lightstreamer_adapter.interfaces.metadata.MetadataProvider \
        adapter: the Remote Metadata Adapter instance to be run.
        :param tuple address: the address of the Proxy Adapter supplied as a
         2-tuple ``(host, request_reply_port)`` where:

         * host: a string representing the hostname or the IP address
         * request_reply_port: an int representing the request/reply port
        :param str name: the name associated to the Server instance.
        :param float keep_alive: the keepalive interval expressed in seconds
         (or fractions)
        :param int thread_pool_size: the thread pool size
        :raises TypeError: if the supplied Remote Adapter is not an instance of
         a subclass of
         :class:`lightstreamer_adapter.interfaces.metadata.MetadataProvider`.
        """
        super(MetadataProviderServer, self).__init__(address, name, keep_alive,
                                                     thread_pool_size)
        if not isinstance(adapter, MetadataProvider):
            raise TypeError(("The provided adapter is not a subclass of "
                             "lightstreamer_adapter.interfaces."
                             "MetadataProvider"))
        self._config_file = None
        self._params = None
        self._adapter = adapter
        self.init_expected = True

    def _on_request_receiver_started(self):
        """Invoked to notify this subclass the the Request Receiver has been
        started.

        This class has a void implementation.
        """
        pass

    def _handle_request(self, request_id, data, method_name):
        init_request = method_name == str(meta_protocol.Method.MPI)
        if init_request and not self.init_expected:
            raise RemotingException("Unexpected late {} request"
                                    .format(str(meta_protocol.Method.MPI)))
        elif not init_request and self.init_expected:
            raise RemotingException(("Unexpected request {} while waiting for "
                                     "{} request")
                                    .format(method_name,
                                            meta_protocol.Method.MPI))
        if init_request:
            self.init_expected = False
            res = self._on_mpi(data)
            self._send_reply(request_id, res)
            return

        # Builds the name of the method_name do be invoked, starting from the
        # protocol method_name name, and retrieves such method_name.
        on_method_name = "_on_" + method_name.lower()
        try:
            on_method = getattr(self, on_method_name)
        except AttributeError:
            METADATA_LOGGER.warning("Discarding unknown request: %s",
                                    method_name)
            return

        # Invokes the retrieved method, which in turn returns an asynchronous
        # function to be executed through the executor-
        async_func = on_method(data)

        # Define a task function to wrap the execution of the returned
        # async_func and the sending of the gotten reply.
        def execute_and_reply():
            try:
                # async_func may raise again a RemotingExcption, which need
                # to be caught and forwarded to the ExceptionHandler.
                reply = async_func()
                self._send_reply(request_id, reply)
            except RemotingException as err:
                self.on_exception(err)

        # Submits the task to the executor for asynchronous execution.
        self._executor.submit(execute_and_reply)

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
                    METADATA_LOGGER.warning(("None or empty field list from "
                                             "get_items for group '%s'"),
                                            group)
            except Exception as err:
                res = meta_protocol.write_get_items(exception=err)
            else:
                res = meta_protocol.write_get_items(items)
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
                    METADATA_LOGGER.warning(("None or empty field list from "
                                             "get_schema for schema '%s' in "
                                             "group '%s'"), schema, group)
            except Exception as err:
                res = meta_protocol.write_get_schema(exception=err)
            else:
                res = meta_protocol.write_get_schema(fields)
            return res
        return execute

    def _on_git(self, data):
        parsed_data = meta_protocol.read_get_item_data(data)

        def execute():
            try:
                items = [{"allowedModeList":
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
                items = [{"allowedModeList":
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
                res = meta_protocol.write_notify_new_tables(err)
            else:
                res = meta_protocol.write_notify_new_tables()
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
        session_id = parsed_data["sessionId"]
        user = parsed_data["user"]

        def execute():
            try:
                self._adapter.notify_mpn_device_access(user, session_id,
                                                       mpn_device_info)
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
        session_id = parsed_data["sessionId"]
        mpn_device_info = parsed_data["mpnDeviceInfo"]
        user = parsed_data["user"]
        new_device_toksn = parsed_data["newDeviceToken"]

        def execute():
            try:
                self._adapter.notify_mpn_device_token_change(user,
                                                             session_id,
                                                             mpn_device_info,
                                                             new_device_toksn)
            except Exception as err:
                res = meta_protocol.write_device_token_change(err)
            else:
                res = meta_protocol.write_device_token_change()
            return res
        return execute

    def _handle_ioexception(self, ioexception):
        METADATA_LOGGER.fatal(("Exception caught while reading/writing from/to"
                               " network: <%s>, aborting..."),
                              str(ioexception))
        super(MetadataProviderServer, self)._handle_ioexception(ioexception)

    def _handle_exception(self, exception):
        METADATA_LOGGER.error("Caught exception: %s", str(exception))
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
        method, to supply optional parameters.

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
        """
        METADATA_LOGGER.info(("Managing Metadata Adapter %s with a thread pool"
                              " size of %d"), self.name, self.thread_pool_size)
        try:
            super(MetadataProviderServer, self).start()
        except (TypeError, OSError) as err:
            raise MetadataProviderError(("Caught an error during the "
                                         "initialization phase")) from err


class DataProviderServer(Server):
    """A server object which can run a Remote Data Adapter and connect
    it to the Proxy Adapter running on Lightstreamer Server.

    The object should be provided with a DataProvider instance and with
    suitable initialization parameters and established connections, then
    activated through :meth:`DataProviderServer.start()` and finally disposed
    through :meth:`Server.close()`.
    Further reuse of the same instance is not supported.

    The server will take care of sending keepalive packets on the connections
    when needed. The interval can be configured through the provided
    ``keep_alive`` parameter; by default it is 1 sec; a value of 0, negative or
    ``None`` disables the keepalives.

    By default, the invocations to the Data Adapter methods will be done in
    a limited thread pool with a size determined by the number of detected cpu
    cores. The size can be specified through the provided
    ``thread_pool_size`` parameter. A size of 1 enforces strictly sequential
    invocations and can be used if parallelization of the calls is not
    supported by the Metadata Adapter. A value of 0, negative or ``None`` also
    implies the default behaviour as stated above.

    Note that :meth;Subscribe and Unsubscribe invocations for the same item are
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
        self._config_file = None
        self._params = None
        self._adapter = adapter
        self._subscription_mgr = SubscriptionManager(self._executor)
        self.init_expected = True
        self._notify_sender = None
        self.notify_address = (address[0], address[2])
        self._notify_sock = None

    def _handle_request(self, request_id, data, method_name):
        init_request = method_name == str(data_protocol.Method.DPI)
        if init_request and not self.init_expected:
            raise RemotingException("Unexpected late {} request"
                                    .format(str(data_protocol.Method.DPI)))
        elif not init_request and self.init_expected:
            raise RemotingException(("Unexpected request {} while waiting for "
                                     "{} request")
                                    .format(method_name,
                                            data_protocol.Method.DPI))
        if init_request:
            self.init_expected = False
            res = self._on_dpi(data)
            self._send_reply(request_id, res)
        elif method_name == "SUB":
            self._on_sub(request_id, data)
        elif method_name == "USB":
            self._on_usb(request_id, data)
        else:
            DATA_LOGGER.warning("Discarding unknown request: %s", method_name)

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
        method, to supply optional parameters.

        :Getter: Returns the dictionary object of optional parameters
        :Setter: Sets the dictionary object of optional parameters
        :type: dict
        """
        return self._params

    @adapter_params.setter
    def adapter_params(self, value):
        self._params = value

    def _on_request_receiver_started(self):
        """Invoked to notify this subclass the the Request Receiver has been
        started.

        This class creates an additional socket to enable the communication
        from the Remote Data Adapter to the Proxy Adapter, in order to send
        data over the notification channel.
        """
        self._notify_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._notify_sock.connect(self.notify_address)
        notify_sender_log = logging.getLogger(("lightstreamer-adapter."
                                               "requestreply.notifications."
                                               "NotifySender"))
        self._notify_sender = _Sender(sock=self._notify_sock, server=self,
                                      log=notify_sender_log)
        self._notify_sender.start()

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
        item_name = data_protocol.read_sub(data)

        def do_task():
            DATA_LOGGER.debug("Processing SUB request: %s", request_id)
            success = False
            try:
                snpt_available = self._adapter.issnapshot_available(item_name)
                if snpt_available is False:
                    self.end_of_snapshot(item_name)
                self._adapter.subscribe(item_name)
                success = True
            except Exception as err:
                res = data_protocol.write_sub(err)
            else:
                res = data_protocol.write_sub()
            self._send_reply(request_id, res)
            return success

        def do_late_task():
            DATA_LOGGER.info("Skipping request: %s", request_id)
            subscribe_err = SubscribeError("Subscribe request come too late")
            res = data_protocol.write_sub(subscribe_err)
            return res

        sub_task = ItemTask(request_id, True, do_task, do_late_task)
        self._subscription_mgr.do_subscription(item_name, sub_task)

    def _on_usb(self, request_id, data):
        item_name = data_protocol.read_usub(data)

        def do_task():
            DATA_LOGGER.debug("Processing USB request: %s", request_id)
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
            DATA_LOGGER.info("Skipping request: %s", request_id)
            res = data_protocol.write_unsub()
            self._send_reply(request_id, res)

        unsub_task = ItemTask(request_id, False, do_task, do_late_task)
        self._subscription_mgr.do_unsubscription(item_name, unsub_task)

    @notify
    def _send_notify(self, ntfy):
        self._notify_sender.send(ntfy)

    def update(self, item_name, events_map, issnapshot):
        request_id = self._subscription_mgr.get_active_item(item_name)
        if request_id:
            try:
                res = data_protocol.write_update_map(item_name, request_id,
                                                     issnapshot, events_map)
                self._send_notify(res)
            except RemotingException as err:
                self.on_exception(err)
        else:
            DATA_LOGGER.warning("Unexpected update for item %s", item_name)

    def end_of_snapshot(self, item_name):
        request_id = self._subscription_mgr.get_active_item(item_name)
        if request_id:
            try:
                res = data_protocol.write_eos(item_name, request_id)
                self._send_notify(res)
            except RemotingException as err:
                self.on_exception(err)
        else:
            DATA_LOGGER.warning(("Unexpected end_of_snapshot notify for item "
                                 "%s"), item_name)

    def clear_snapshot(self, item_name):
        request_id = self._subscription_mgr.get_active_item(item_name)
        if request_id:
            try:
                res = data_protocol.write_cls(item_name, request_id)
                self._send_notify(res)
            except RemotingException as err:
                self.on_exception(err)
        else:
            DATA_LOGGER.warning(("Unexpected clear_snapshot for item %s"),
                                item_name)

    def failure(self, exception):
        try:
            res = data_protocol.write_failure(exception)
            self._send_notify(res)
        except RemotingException as err:
            self.on_exception(err)

    def _handle_ioexception(self, ioexception):
        DATA_LOGGER.fatal(("Exception caught while reading/writing from/to "
                           "network: <%s>, aborting..."), str(ioexception))
        super(DataProviderServer, self)._handle_ioexception(ioexception)

    def _handle_exception(self, exception):
        DATA_LOGGER.error(("Caught exception: %s, trying to notify a failure.."
                           "."), str(exception))
        try:
            res = data_protocol.write_failure(exception)
            self._send_notify(res)
        except RemotingException:
            DATA_LOGGER.exception(("Caught second-level exception while trying"
                                   " to notify a first-level exception"))

        return False

    def start(self):
        """Starts the Remote Data Adapter. A connection to the Proxy Adapter is
        performed (as soon as one is available). Then, requests issued by the
        Proxy Adapter are received and forwarded to the Remote Adapter.

        :raises lightstreamer_adapter.interfaces.data.DataProviderError: If an
         error occurred in the initialization phase. The adapter was not
         started.
        """
        DATA_LOGGER.info(("Managing Data Adapter %s with a thread pool size of"
                          " %d"), self.name, self.thread_pool_size)
        try:
            super(DataProviderServer, self).start()
        except (TypeError, OSError) as err:
            raise DataProviderError(("Caught an error during the "
                                     "initialization phase")) from err

    def close(self):
        """Stops the management of the Remote Data Adapter attached to this
        Server object.
        The method first invokes the inherited Server.close() and then closes
        the Notify Sender object.
        """
        super(DataProviderServer, self).close()
        self._notify_sock.close()
        self._notify_sender.quit()


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
