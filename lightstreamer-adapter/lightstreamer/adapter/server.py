
import sys
import socket
import queue
import logging
import threading
import time
import multiprocessing

from abc import ABCMeta, abstractmethod

import lightstreamer.adapter.protocol as protocol
import lightstreamer.adapter.metadata_protocol as meta_protocol
import lightstreamer.adapter.data_protocol as data_protocol
from lightstreamer.interfaces.data import (ItemEventListener,
                                           DataProvider, SubscribeError)
from lightstreamer.interfaces.metadata import Mode, MetadataProvider
from _collections import deque
from lightstreamer.adapter.protocol import RemotingException

__all__ = ['Server', 'DataProviderServer', 'MetadataProviderServer',
           'ExceptionHandler']
log = logging.getLogger(name="Remote Adapter")
server_log = logging.getLogger(name="lightstreamer.adapters.server")
dataprovider_log = logging.getLogger(("lightstreamer.adapters.server."
                                      "DataProviderServer"))


def _receive(sock):
    data = b''
    while True:
        more = sock.recv(1024)
        if not more:
            raise EOFError('Socket connection broken')
        data += more
        if (data.endswith(b'\n')):
            logging.debug("Arrived {}".format(len(more)))
            break

    return data.decode()


class Task():
    pass


class _Worker(threading.Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, name, tasks):
        super(_Worker, self).__init__(name=name)
        self.tasks = tasks
        self.daemon = False
        self._executing = threading.Event()
        self.start()
        log.debug("Started Worker {}".format(name))

    def run(self):
        while True:
            if self._executing.isSet():
                break
            task = self.tasks.get()
            try:
                if task == "END":
                    print("Terminated {}".format(self.name))
                    break
                print("Running from {}".format(self.name))
                task()  # func(*args, **kargs)
            except Exception as e:
                print(e)
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
        def t():
            func(*args, **kargs)

        self.pool.put(t)

    def join(self):
        dismissed = []
        for worker in self.workers:
            self.pool.put("END")
            dismissed.append(worker)

        for worker in dismissed:
            worker.join()

        self.pool.join()


def notify(f):
    def wrap(obj, reply):
        timestamp = int(round(time.time() * 1000))
        notification = "|".join([str(timestamp), reply])
        f(obj, notification)

    return wrap


class _reply_handler(object):

    def __init__(self, sock):
        self._server_sock = sock

    def __call__(self, reply):
        log.debug("Reply = {}".format(reply))
        self._server_sock.sendall(bytes(reply + '\r\n', 'utf-8'))


class _NotifySender(object):

    def __init__(self, handler, name, keep_alive):
        self.handler = handler
        self.name = name
        self.keep_alive = keep_alive
        # self._keep_alive = 0

    def start(self):
        self.queue = queue.Queue()
        self._notify_thread = threading.Thread(target=self.run,
                                               name=self.name)
        self._notify_thread.start()

    def send_notify(self, notify):
        self.queue.put(notify)

    def run(self):
        while True:
            try:
                if self.keep_alive > 0:
                    log.debug("Getting new message to notify with timeout")
                    try:
                        notification = self.queue.get(timeout=self.keep_alive)
                        self.queue.task_done()
                    except queue.Empty:
                        log.exception("Keep alive triggered...")
                        notification = "KEEPALIVE"
                else:
                    log.debug("Getting new message to notify...")
                    notification = self.queue.get()
                    self.queue.task_done()
                if notification == "STOP":
                    break
                self.handler(notification)
            finally:
                pass

    def stop(self):
        self.queue.put("STOP")
        self.queue.join()
        self._notify_thread.join()
        log.info("_NotifySender stopped")


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

    DEFAULT_POOL_SIZE = 4

    _number = 0

    def __init__(self, address, name, keep_alive, thread_pool_size):
        Server._number += 1
        self._exception_handler = None
        self._address = address
        self._name = "#{}".format(Server._number) if name is None else name
        self._keep_alive = max(0, keep_alive) if keep_alive is not None else 0
        pool = max(0, thread_pool_size) if thread_pool_size is not None else 0
        if pool == 0:
            try:
                self._thread_pool_size = multiprocessing.cpu_count()
            except NotImplementedError:
                self._thread_pool_size = Server.DEFAULT_POOL_SIZE
        else:
            self._thread_pool_size = pool

    @property
    def name(self):
        """The name, used for logging purposes, associated to the Server
        instance.

        :type: str
        """
        return self._name

    @property
    def keep_alive(self):
        """The keepalive interval expressed in seconds (or fractions)

        :type: float
        """
        return self._keep_alive

    @property
    def thread_pool_size(self):
        """The thread pool size

        :type: int
        """
        return self._thread_pool_size

    def set_exception_handler(self, handler):
        """Sets the handler for error conditions occurring on the Remote
        Server. By setting the handler, it's possible to override the default
        exception handling.

        :param lightstreamer.adapter.server.ExceptionHandler handler: the
         handler for error conditions occurring on the Remote Server.
        """
        self._exception_handler = handler

    @abstractmethod
    def start(self):
        if self._keep_alive > 0:
            server_log.info("Keepalive time for {} set to {} milliseconds"
                            .format(self._name, self._keep_alive))
        else:
            server_log.info("Keepalive for {} disabled".format(self._name))

        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.connect(self._address)
        self._stop_request = threading.Event()

        # Start the Thread Pool
        self.thread_pool = _ThreadPool(self._thread_pool_size)
        # Setup and start the receiver thread.
        self._receiver_thread = threading.Thread(target=self.run,
                                                 name="Receiver",
                                                 args=(self._server_sock,))
        self._receiver_thread.start()

        # Setup and start the notify sender object
        self._notifyer = _NotifySender(_reply_handler(self._server_sock),
                                       "Replyer",
                                       self._keep_alive)
        self._notifyer.start()
        self._on_request_reply_started()

    def close(self):
        """Stops the management of the Remote Adapter and destroys the threads
        used by this Server. This instance can no longer be used.

        Note that this does not stop the supplied Remote Adapter, as no close
        method is available in the Remote Adapter interface. If the process is
        not terminating, then the Remote Adapter cleanup should be performed by
        accessing the supplied Adapter instance directly and calling custom
        methods.
        """
        self._stop_request.set()
        self._server_sock.close()
        self._receiver_thread.join()

        self._notifyer.stop()
        self.thread_pool.join()

    def run(self, socket):
        server_log.info("Request receiver thread {} starting..."
                        .format(self._name))
        while not self._stop_request.is_set():
            log.debug("Waiting for new incoming request...")
            request = None
            try:
                data = b''
                while True:
                    server_log.debug("Reading from socket...")
                    more = socket.recv(1024)
                    server_log.debug("Received %d test_data", len(more))
                    if not more:
                        raise EOFError('Socket connection broken')
                    data += more
                    if data.endswith(b'\n'):
                        break
                request = data.decode()
            except Exception as e:
                if self._stop_request.is_set():
                    server_log.debug(("Error raised because of explicitly "
                                      "closed socket, no issue"))
                    continue
                server_log.exception(("Exception while consuming data from the"
                                     " socket"))
                self._on_ioexception(e)
                break

            server_log.debug("Request line: {}".format(request))
            self._on_received_request(request)
        server_log.info("Request receiver thread {} stopped"
                        .format(self._name))

    def _on_received_request(self, request):
        try:
            parsed_request = protocol.parse_request(request)
            if parsed_request is None:
                server_log.warn("Discarding malformed request")
                return

            request_id = parsed_request["id"]
            method = parsed_request["method"]
            data = parsed_request["test_data"]
            self._handle_request(request_id, data, method, request)
        except Exception as e:
            server_log.exception("Exception while handling a request")
            self._on_exception(e)

    def _send_reply(self, request_id, response):
        if response is not None:
            reply = '|'.join((request_id, response))
            self._notifyer.send_notify(reply)

    def _on_ioexception(self, ioexception):
        if self._exception_handler is not None:
            # Enable default handling in case of False
            server_log.info(("Caught exception: {}, notifying the "
                             "application...").format(str(ioexception)))
            if not self._exception_handler.handle_ioexception(ioexception):
                return

        self._handle_ioexception(ioexception)

    def _on_exception(self, exception):
        if self._exception_handler is not None:
            # Enable default handling in case of False
            server_log.info(("Caught exception: {}, notifying the "
                             "application...").format(str(exception)))
            if not self._exception_handler.handle_exception(exception):
                return

        self._handle_exception(exception)

    @abstractmethod
    def _handle_ioexception(self, ioexception):
        sys.exit(1)
        return False

    def _handle_exception(self, ioexception):
        pass

    def _on_request_reply_started(self):
        '''Method meant to be overriden by subclasses'''
        pass


class MetadataProviderServer(Server):
    """A server object which can run a Remote Metadata Adapter and connect it
    to the Proxy Adapter running on Lightstreamer Server.

    The object should be provided with a MetadataProvider instance and with
    suitable initialization parameters and established connections,
    then activated through
    :meth:`lightstreamer.adapter.server.MetadataProviderServer.start` and
    finally disposed through :meth:`lightstreamer.adapter.server.Server.close`.
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
        :meth:'lightstreamer.interfaces.metadata.MetadataAdapter.initialize'
        method of the Remote Adapter will be invoked only upon a Proxy Adapter
        request.

        :param lightstreamer.interfaces.metadata.MetadataProvider adapter: the
         Remote Metadata Adapter instance to be run.
        :param tuple address: the address of the Proxy Adapter supplied as a
         2-tuple ``(host, request_reply_port)`` where:

         * host: a string representing the hostname or the IP address
         * request_reply_port: an int representing the request/reply port
        :raises TypeError: if the supplied Remote Adapter is not an instance of
         a subclass of
         :class:`lightstreamer.interfaces.metadata.MetadataProvider`.
        """
        super(MetadataProviderServer, self).__init__(address, name, keep_alive,
                                                     thread_pool_size)
        if not isinstance(adapter, MetadataProvider):
            raise TypeError(("The provided adapter is not a subclass of "
                             "lightstreamer.interfaces.MetadatadataProvider"))
        self._config_file = None
        self._params = None
        self._adapter = adapter
        self.init_expected = True

    def _handle_request(self, request_id, data, method, request):
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
            res = self._on_mpi(request_id, data)
            self._send_reply(request_id, res)
            return
        # Build the name of the method do be invoked, starting from the
        # protocol method name.
        on_method = "_on_" + method.lower()
        # Invokes the method and gets the asynchronous function to be executed
        # through the thread pool.
        async_func = getattr(self, on_method)(request_id, data)
        # Define the async function which wraps the invocation to
        # async_func and decorates its response.

        def async_task():
            try:
                response = async_func()
                self._send_reply(request_id, response)
                server_log.info("Request {} dispatched".format(request_id))
            except Exception as err:
                self._on_exception(err)

        self.thread_pool.submit(async_task)

    def _on_mpi(self, request_id, data):
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

    def _on_nus(self, request_id, data):
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

    def _on_nua(self, request_id, data):
        parsed_data = meta_protocol.read_notify_user_auth(data)
        user = parsed_data["user"]
        http_headers = parsed_data["httpHeaders"]
        password = parsed_data["password"]
        client_principal = parsed_data["clientPrincipal"]

        def execute():
            try:
                self._adapter.notify_user_with_client_principal(
                                                              user,
                                                              password,
                                                              http_headers,
                                                              client_principal)
                max_bandwidth = self._adapter.get_allowed_max_bandwidth(user)
                wants_tb_notify = self._adapter.wants_tables_notification(user)
            except Exception as err:
                res = meta_protocol.write_notiy_user(
                                                      meta_protocol.Method.NUA,
                                                      exception=err)
            else:
                res = meta_protocol.write_notiy_user(
                                                      meta_protocol.Method.NUA,
                                                      max_bandwidth,
                                                      wants_tb_notify)
            return res
        return execute

    def _on_nns(self, request_id, data):
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

    def _on_nsc(self, requestId, data):
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

    def _on_gis(self, request_id, data):
        parsed_data = meta_protocol.read_get_items(data)
        session_id = parsed_data["session_id"]
        user = parsed_data["user"]
        group = parsed_data["group"]

        def execute():
            try:
                items = self._adapter.get_items(user, session_id, group)
                res = meta_protocol.write_get_items(items)
            except Exception as err:
                res = meta_protocol.write_get_items(exception=err)
            return res
        return execute

    def _on_gsc(self, request_id, data):
        parsed_data = meta_protocol.read_get_schema(data)
        session_id = parsed_data["session_id"]
        group = parsed_data["group"]
        user = parsed_data["user"]
        schema = parsed_data["schema"]

        def execute():
            try:
                fields = self._adapter.get_schema(user, session_id, group,
                                                 schema)
                res = meta_protocol.write_get_schema(fields)
            except Exception as err:
                res = meta_protocol.write_get_schema(exception=err)

            return res
        return execute

    def _on_git(self, request_id, data):
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

    def _on_gui(self, request_id, data):
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

    def _on_num(self, request_id, data):
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

    def _on_nnt(self, request_id, data):
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

    def _on_ntc(self, request_id, data):
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

    def _on_mda(self, request_id, data):
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

    def _on_msa(self, request_id, data):
        parsed_data = meta_protocol.read_notify_subscription_activation(data)
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
                res = meta_protocol.write_notify_subscription_activation(err)
            else:
                res = meta_protocol.write_notify_subscription_activation()
            return res
        return execute

    def _on_mdc(self, request_id, data):
        parsed_data = meta_protocol.read_notify_device_token_change(data)
        mpn_device_info = parsed_data["mpnDeviceInfo"]
        user = parsed_data["user"]
        new_device_toksn = parsed_data["newDeviceToken"]

        def execute():
            try:
                self._adapter.notify_mpn_device_token_change(user,
                                                            mpn_device_info,
                                                            new_device_toksn)
            except Exception as err:
                res = meta_protocol.write_notify_device_token_change(err)
            else:
                res = meta_protocol.write_notify_device_token_change()
            return res
        return execute

    def _handle_ioexception(self, ioexception):
        log.fatal(("Exception caught while reading/writing from/to "
                   "network: <{}>, aborting...").format(str(ioexception)))
        super(MetadataProviderServer, self)._handle_ioexception(ioexception)

    def _handle_exception(self, exception):
        log.error("Caught exception: {}".format(str(exception)))
        return False

    @property
    def adapter_config(self):
        """The pathname of an optional configuration file for the Remote
        Metadata Adapter, to be passed to the
        :meth:``lightstreamer.interfaces.metadata.MetadataProvider.initialize``
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
        :meth:`lightstreamer.interfaces.metadata.MetadataProvider.initialize`
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

        :raises lightstreamer.interfaces.metadata.MetadataProviderError: If an
         error occurred in the initialization phase. The adapter was not
         started.
        :raises Exception: if an error occurred in the initialization phase.
         The adapter was not started.
        """
        super(MetadataProviderServer, self).start()


class _ItemEventListeer(ItemEventListener):

    def update(self, item_name, item_event, issnapshot):
        request_id = self._get_active_item(item_name)
        if request_id:
            try:
                res = data_protocol.write_update_map(item_name, request_id,
                                                     issnapshot, item_event)
                self._send_notify(res)
            except protocol.RemotingException as e:
                self._on_exception(e)
        else:
            log.warn("Unexpected update for item {}".foramt(item_name))
        ItemEventListener.update(self, item_name, item_event, issnapshot)

    def end_of_snapshot(self, item_name):
        ItemEventListener.end_of_snapshot(self, item_name)

    def clear_snapshot(self, item_name):
        ItemEventListener.clear_snapshot(self, item_name)

    def failure(self, exception):
        ItemEventListener.failure(self, exception)


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
        self._lock = threading.Lock()
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
            except Exception as e:
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
    :meth:`lightstreamer.adapter.server.DataProviderServer.start()`
    and finally disposed through
    :meth:`lightstreamer.adapter.server.Server.close()`.
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

        :param lightstreamer.interfaces.data.DataProvider adapter: The Remote
         Adapter instance to be run.
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
         a subclass of :class:`lightstreamer.interfaces.data.DataProvider`.
        """
        super(DataProviderServer, self).__init__((address[0], address[1]),
                                                 name,
                                                 keep_alive,
                                                 thread_pool_size)

        if not isinstance(adapter, DataProvider):
            raise TypeError(("The provided adapter is not a subclass of "
                             "lightstreamer.interfaces.DataProvider"))
        self._config_file = None
        self._params = None
        self._adapter = adapter
        self.init_expected = True
        self.active_items = {}
        self.items_lock = threading.RLock()
        self.notify_address = (address[0], address[2])
        self.notify_sock = None

    def _handle_request(self, request_id, data, method, request):
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
            res = self._on_dpi(request_id, data)
            self._send_reply(request_id, res)
        elif method == "SUB":
            self._on_sub(request_id, data)
        elif method == "USB":
            self._on_usb(request_id, data)
        else:
            dataprovider_log.warn("Discarding unknown request: {}"
                                  .format(request))

    @property
    def adapter_config(self):
        """The pathname of an optional configuration file for the Remote
        Data Adapter, to be passed to the
        :meth:``lightstreamer.interfaces.data.DataProvider.initialize``
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
        :meth:`lightstreamer.interfaces.data.DataProvider.initialize`
        method of the Remote Metadata Adapter, to supply optional parameters.

        :Getter: Returns the dictionary object of optional parameters
        :Setter: Sets the dictionary object of optional parameters
        :type: dict
        """
        return self._params

    @adapter_params.setter
    def adapter_params(self, value):
        self._params = value

    def _on_request_reply_started(self):
        self.notify_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.notify_sock.connect(self.notify_address)
        notify_handler = _reply_handler(self.notify_sock)
        self._notify_sender = _NotifySender(handler=notify_handler,
                                            name="Notifier",
                                            keep_alive=self.keep_alive)
        self._notify_sender.start()

    def _get_active_item(self, item_name):
        with self.items_lock:
            if item_name in self.active_items:
                item_manager = self.active_items[item_name]
                return item_manager.code

    def _on_dpi(self, request_id, data):
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
                    self.end_of_snapshot(item, request_id)
                self._adapter.subscribe(item)
                success = True
            except Exception as err:
                res = data_protocol.write_sub(err)
            else:
                res = data_protocol.write_sub()
            self._send_reply(request_id, res)
            return success

        def do_late_task():
            dataprovider_log.info("Skipping request: {}".format(request_id))
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
            dataprovider_log.info("Skipping request: {}".format(request_id))
            res = data_protocol.write_unsub()
            self._send_reply(request_id, res)

        unsub_task = _ItemTask(request_id, False, do_task, do_late_task)
        with self.items_lock:
            if item_name not in self.active_items:
                dataprovider_log.error("Task list expected for item {}"
                                       .format(item_name))
                return
            item_manager = self.active_items[item_name]
            item_manager.queued += 1
        item_manager.add_task(unsub_task)

    @notify
    def _send_notify(self, notify):
        self._notify_sender.send_notify(notify)

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
            dataprovider_log.warn("Unexpected update for item {}"
                                  .format(item_name))

    def end_of_snapshot(self, item_name):
        request_id = self._get_active_item(item_name)
        if request_id:
            try:
                res = data_protocol.write_eos(item_name, request_id)
                self._send_notify(res)
            except protocol.RemotingException as err:
                self._on_exception(err)
        else:
            dataprovider_log.warn(("Unexpected end_of_snapshot notify for "
                                   "item {}").format(item_name))

    def clear_snapshot(self, item_name):
        request_id = self._get_active_item(item_name)
        if request_id:
            try:
                res = data_protocol.write_cls(item_name, request_id)
                self._send_notify(res)
            except protocol.RemotingException as err:
                self._on_exception(err)
        else:
            dataprovider_log.warn("Unexpected clear_snapshot for item {}"
                                  .format(item_name))

    def failure(self, exception):
        try:
            res = data_protocol.write_failure(exception)
            self._send_notify(res)
        except protocol.RemotingException as err:
            self._on_exception(err)

    def _handle_ioexception(self, ioexception):
        log.fatal(("Exception caught while reading/writing from/to "
                   "network: <{}>, aborting...").format(str(ioexception)))
        super(DataProviderServer, self)._handle_ioexception(ioexception)

    def _handle_exception(self, exception):
        dataprovider_log.error(("Caught exception: {}, trying to notify a "
                                "failure...").format(str(exception)))
        try:
            res = data_protocol.write_failure(exception)
            self._send_notify(res)
        except Exception:
            dataprovider_log.exception(("Caught second-level exception while "
                                        "trying to notify a first-level "
                                        "exception"))

        return False

    def start(self):
        """Starts the Remote Data Adapter. A connection to the Proxy Adapter is
        performed (as soon as one is available). Then, requests issued by the
        Proxy Adapter are received and forwarded to the Remote Adapter.

        :raises lightstreamer.interfaces.data.DataProviderError: If an error
         occurred in the initialization phase. The adapter was not started.
        :raises Exception: if an error occurred in the initialization phase.
         The adapter was not started.
        """
        super(DataProviderServer, self).start()

    def close(self):
        super(DataProviderServer, self).close()
        self._notify_sender.stop()
        dataprovider_log.info("Stopped")


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
