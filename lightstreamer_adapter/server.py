"""
Core module of the Lightstreamer SDK for Python Adapters, containing all
classes (public and private), needed to configure and start the Remote
Adapters.
"""
import socket
import queue
import logging
import time
import traceback
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


def create_socket_and_connect(address, ssl_context=None):
    """Connect to the Proxy Adapter listening on the provided address, and
    return the socket object.
    If an SSLContext is specified, the connection is established by using the
    wrapped socket.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if ssl_context is not None:
        hostname = address[0]
        client_socket = ssl_context.wrap_socket(sock, server_hostname=hostname)
    else:
        client_socket = sock
    client_socket.connect(address)
    return client_socket


class _Sender():
    """Helper class which manages the communications from the Remote Adapter to
    the ProxyAdapter, sending data over the "requests/replies" channels and/or
    the "notifications" subchannel.
    """

    _STOP_WAITING_PILL = "STOP_WAITING_PILL"

    _KEEPALIVE_PILL = "KEEPALIVE_PILL"

    def __init__(self, name, sock, server, keepalive, log):
        self._sock = sock
        self._server = server
        self._name = name
        self._log = log
        self._keepalive = keepalive
        self._keep_alive_log = log
        self._send_queue = None
        self._send_thread = None
        self._notification_log = log

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
                                   .format(self._name))
        self._send_thread.start()

    def send(self, message, is_notif):
        """Enqueues a reply or notification to be sent to the Proxy Adapter."""
        if is_notif:
            self._notification_log.debug("%s Enqueing line: %s",
                                         self._name, message)
        else:
            self._log.debug("%s Enqueing line: %s", self._name, message)
        self._send_queue.put(message)

    def _do_run(self):
        """Target method for the Sender-Thread-XXX, started in the start'
        method."""
        self._log.info("%s starting", self._name)
        while True:
            try:
                to_send = None
                current_log = self._log
                current_log.debug("%s Waiting for a line to send...",
                                  self._name)
                if self._keepalive > 0:
                    try:
                        to_send = self._send_queue.get(timeout=self._keepalive)
                    except queue.Empty:
                        # Keepalive Timeout triggered.
                        to_send = protocol.Method.KEEPALIVE.name
                        current_log = self._keep_alive_log
                else:
                    to_send = self._send_queue.get()

                if to_send == _Sender._STOP_WAITING_PILL:
                    # Request of stopping dequeuing, thread termination.
                    break

                if to_send is None or to_send == _Sender._KEEPALIVE_PILL:
                    to_send = protocol.Method.KEEPALIVE.name
                    current_log = self._keep_alive_log

                # Send to_send over the network.
                current_log.debug("%s Sending line: %s", self._name,  to_send)
                self._sock.sendall(bytes(to_send + '\r\n', 'utf-8'))
            except OSError as err:
                self._server.on_ioexception(err)
                break
            except Exception as err:
                self._server.on_exception(err)
                break
        self._log.info("'%s' stopped", self._name)

    def change_keep_alive(self, keepalive, also_interrupt=False):
        self._keepalive = keepalive
        self._log.debug("%s Changing keepalive to: %d", self._name,
                        self._keepalive)
        if also_interrupt:
            # Interrupts the current wait as though a keepalive were needed;
            # in most cases, this keepalive will be redundant
            self._send_queue.put(_Sender._KEEPALIVE_PILL)

    def quit(self):
        """Terminates the communications with the Proxy Adapter."""
        # Enqueues a None item to notify the Sender-Thread-XXX of stopping
        # dequeuing of incoming replies/notifications.
        self._send_queue.put(_Sender._STOP_WAITING_PILL)
        self._send_thread.join()


class _RequestManager():
    """Helper class which manages the bi-directional communications with the
    Proxy Adpater counterpart over the "requests/replies" channels.
    """

    def __init__(self, sock, keepalive, server):
        self._log = logging.getLogger("lightstreamer-adapter.requestreply"
                                      ".requests")
        self._sock = sock
        self._server = server
        reply_sender_log = logging.getLogger("lightstreamer-adapter."
                                             "requestreply.replies")
        keep_alive_log = logging.getLogger("lightstreamer-adapter."
                                           "requestreply.keep_alives")
        self._reply_sender = _Sender(sock=sock, name=self._server.name,
                                     server=self._server,
                                     keepalive=keepalive, log=reply_sender_log)
        self._reply_sender._keep_alive_log = keep_alive_log
        self._stop_request = Event()

        # Starts the reply sender.
        self._reply_sender.start()

    def startReceiving(self):
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

    def _do_run(self, sock):
        """Target method for the RequestReceiver-Thread-XXX, started in the
        'start' method.
        """
        self._log.info("Request receiver '%s' starting...", self._server.name)
        buffer = ''
        while not self._stop_request.is_set():
            try:
                self._log.debug("%s Reading from socket...",
                                self._server.name)
                data = sock.recv(1024)
                self._log.debug("%s Received %d bytes of request data [%s]",
                                self._server.name, len(data), data)
                if not data:
                    raise EOFError('Socket connection broken')
                buffer += data.decode('ascii')
                self._log.debug("%s Current buffer [%s]", self._server.name,
                                buffer)
                tokens = buffer.splitlines(keepends=True)
                buffer = ''
                for token in tokens:
                    if token.endswith('\n'):
                        self._log.debug("%s Request line: %s",
                                        self._server.name, token)
                        self._server.on_received_request(token)
                        buffer = ''
                    else:
                        self._log.debug("'%s' Buffering remaining token %s",
                                        self._server.name, token)
                        buffer = token
            except (OSError, EOFError) as err:
                if self._stop_request.is_set():
                    self._log.debug("'%s' Error raised because of explicitly "
                                    "closed socket, no issue",
                                    self._server.name)
                    break
                # An exception has been raised, due to some issue in the
                # network communication.
                self._server.on_ioexception(err)
                break
            except Exception as err:
                self._server.on_exception(err)
                break

        self._log.info("Request receiver '%s' stopped", self._server.name)

    def send_reply(self, request_id, response):
        """Sends a reply to the Proxy Adapter.
        """
        reply = '|'.join((request_id, response))
        self._reply_sender.send(reply, False)

    def send_notify(self, ntfy):
        """Sends a notification to the Proxy Adapter.
        """
        self._reply_sender.send(ntfy, True)

    def change_keep_alive(self, keepalive):
        self._reply_sender.change_keep_alive(keepalive)

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
    Remote Server object, which can run a Remote Data or Metadata Adapter and
    connect it to the Proxy Adapter running on Lightstreamer Server.

    The object should be provided with a suitable Adapter instance and with
    suitable initialization parameters, then activated through its own
    :meth:`start` and finally disposed through its own :meth:`close`. Further
    reuse of the same instance is not supported.

    The Remote Server will take care of sending keepalive packets on the
    connections when needed. The interval can be configured through the
    provider ``keep_alive`` parameter, where a value of 0 or negative means no
    keepalives. By default, it is set to 10 sec.

    However, if a stricter interval is requested by the Proxy Adapter on
    startup, it  will be obeyed (with a safety minimum of 1 second). This
    should ensure that the Proxy Adapter activity checks will always succeed,
    but for some old versions of the Proxy Adapter.
    """

    _DEFAULT_POOL_SIZE = 4

    _STRICT_KEEPALIVE = 1000

    _DEFAULT_KEEPALIVE = 10000

    _MIN_KEEPALIVE = 1000

    # Number of current instances of Server' subclasses.
    _number = 0

    def __init__(self, address, name, keep_alive, thread_pool_size,
                 ssl_context):
        Server._number += 1

        # Logger actually overridden by subclasses.
        self._log = logging.getLogger("lightstreamer-adapter.server")
        self._exception_handler = None
        self._remote_user = None
        self._remote_password = None
        self._close_expected = True
        self._config = {}
        self._config['address'] = address
        self._config['name'] = "#{}".format(Server._number) if (name is
                                                                None) else name
        self._configured_keep_alive = keep_alive * 1000 if (keep_alive is not
                                                            None) else None
        self._config['keep_alive'] = max(0, keep_alive) if (keep_alive is not
                                  None) else Server._DEFAULT_KEEPALIVE / 1000

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
        self._request_manager = None
        self._ssl_context = ssl_context

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
    def remote_user(self):
        """The username credential to be sent to the Proxy Adapter upon
        connection. The credentials are needed only if the Proxy Adapter is
        configured to require Remote Adapter authentication.

        :type: str
        """
        return self._remote_user

    @remote_user.setter
    def remote_user(self, value):
        self._remote_user = value

    @property
    def remote_password(self):
        """The password credential to be sent to the Proxy Adapter upon
        connection. The credentials are needed only if the Proxy Adapter is
        configured to require Remote Adapter authentication.

        :type: str
        """
        return self._remote_password

    @remote_password.setter
    def remote_password(self, value):
        self._remote_password = value

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

    def _on_init(self, subprotocol, params, config_file, data, adapter,
                 invoke_listener=False):

        max_version = '1.8.3'
        parsed_data = subprotocol.read_init(data)
        parsed_data.setdefault(protocol.ARI_VERSION, None)
        parsed_data.setdefault(protocol.KEEPALIVE_HINTS, None)
        proxy_version = parsed_data[protocol.ARI_VERSION]
        keep_alive_hint = parsed_data[protocol.KEEPALIVE_HINTS]
        advertised_version = None
        del parsed_data[protocol.ARI_VERSION]
        del parsed_data[protocol.KEEPALIVE_HINTS]
        try:
            if proxy_version is None:
                proxy_version = '1.8.0'
                self._log.info("Received no Proxy Adapter protocol version "
                               "information; assuming 1.8.0.")
            elif proxy_version == '1.8.0':
                raise Exception("Unexpected protocol version number: {}"
                                .format(proxy_version))
            elif proxy_version == '1.8.1':
                raise Exception("Unsupported reserved protocol version "
                                "number: {}".format(proxy_version))
            advertised_version = self.getSupportedVersion(proxy_version, max_version)

            if params is not None:
                init_params = params.copy()
                parsed_data.update(init_params)

            adapter.initialize(parsed_data, config_file)
            if invoke_listener is True:
                adapter.set_listener(self)
        except Exception as err:
            res = subprotocol.write_init(exception=err)
        else:
            proxy_parameters = None
            if advertised_version in ('1.8.0', '1.8.2'):
                self._close_expected = False
            if advertised_version != '1.8.0':
                proxy_parameters = {}
                proxy_parameters[protocol.ARI_VERSION] = advertised_version
            res = subprotocol.write_init(proxy_parameters)
        self._use_keep_alive_hint(keep_alive_hint)
        return res

    @abstractmethod
    def getSupportedVersion(self, proxy_version, max_version):
        return None

    def _use_keep_alive_hint(self, keepalive_hint=None):
        if keepalive_hint is None:
            # No information provided, we stick to a stricter default
            if self._configured_keep_alive is None:
                self._log.info("Keepalive time for %s finally set to %d "
                               "milliseconds to support old Proxy Adapter",
                               self.name, self._STRICT_KEEPALIVE)
                self._change_keep_alive(Server._STRICT_KEEPALIVE)
            # else:
                # For backward compatibility we keep the setting; it is
                # possible that the setting is too long and the Proxy Adapter
                # activity check is triggered
        else:
            keepalive_time = float(keepalive_hint)
            if keepalive_time <= 0:
                pass
                # No restrictions, so our default is still meaningful
            elif self._configured_keep_alive is None:
                if keepalive_time < Server._DEFAULT_KEEPALIVE:
                    if keepalive_time >= Server._MIN_KEEPALIVE:
                        self._log.info("Keepalive time for %s finally set to "
                                       "%d milliseconds as per Proxy Adapter "
                                       "suggestion", self.name, keepalive_time)

                        self._change_keep_alive(keepalive_time)
                    else:
                        self._log.warning("Keepalive time for %s finally set "
                                          "to %d milliseconds, despite a Proxy"
                                          " Adapter suggestion of %d "
                                          "milliseconds", self.name,
                                          Server._MIN_KEEPALIVE,
                                          keepalive_time)
                        self._change_keep_alive(Server._MIN_KEEPALIVE)
                else:
                    # The default setting is stricter, so it's ok
                    self._log.info("Keepalive time for %s finally confirmed to"
                                   " %d milliseconds consistently with Proxy "
                                   "Adapter suggestion", self.name,
                                   Server._DEFAULT_KEEPALIVE)
            elif self._configured_keep_alive > 0:
                if keepalive_time < self._configured_keep_alive:
                    if keepalive_time >= Server._MIN_KEEPALIVE:
                        self._log.warning("Keepalive time for %s changed to %d"
                                          " milliseconds as per Proxy Adapter "
                                          "suggestion", self.name,
                                          keepalive_time)
                        self._change_keep_alive(keepalive_time)
                    else:
                        self._log.warning("Keepalive time for %s changed to %d"
                                          " milliseconds, despite a Proxy "
                                          "Adapter suggestion of %d "
                                          "milliseconds", self.name,
                                          Server._MIN_KEEPALIVE,
                                          keepalive_time)
                        self._change_keep_alive(Server._MIN_KEEPALIVE)
                else:
                    # Our setting is stricter, so it's ok
                    pass
            else:
                if keepalive_time >= Server._MIN_KEEPALIVE:
                    self._log.warning("Keepalives for %s forced with time %d "
                                      "milliseconds as per Proxy Adapter "
                                      "suggestion", self.name, keepalive_time)
                    self._change_keep_alive(keepalive_time)
                else:
                    self._log.warning("Keepalives for %s forced with time %d "
                                      "milliseconds, despite a Proxy Adapter "
                                      "suggestion of %d milliseconds",
                                      self.name, Server._MIN_KEEPALIVE,
                                      keepalive_time)

    def _change_keep_alive(self, keep_alive_milliseconds):
        keep_alive_seconds = keep_alive_milliseconds / 1000
        self._config['keep_alive'] = keep_alive_seconds
        self._request_manager.change_keep_alive(keep_alive_seconds)

    @abstractmethod
    def start(self):
        """Starts the Remote Adapter. A connection to the Proxy Adapter is
        performed (as soon as one is available). Then, requests issued by
        the Proxy Adapter are received and forwarded to the Remote Adapter.
        """
        if self.keep_alive > 0:
            self._log.info("Keepalive time for %s set to %d seconds",
                           self.name, self.keep_alive)
        else:
            self._log.info("Keepalive for %s disabled", self.name)

        self._server_sock = create_socket_and_connect(self._config['address'],
                                                      self._ssl_context)

        # Creates and starts the Request Manager.
        self._request_manager = _RequestManager(sock=self._server_sock,
                                                keepalive=self.keep_alive,
                                                server=self)
        self._send_remote_credentials()
        self._request_manager.startReceiving()

        # Invokes hook to notify subclass that the Request Manager has been
        # started.
        self._on_request_manager_started()

    def close(self):
        """Stops the management of the Remote Adapter and destroys the threads
        used by this Server. This instance can no longer be used.

        Note that this does not stop the supplied Remote Adapter, as no close
        method is available in the Remote Adapter interface. If the process is
        not terminating, then the Remote Adapter cleanup should be performed by
        accessing the supplied Adapter instance directly and calling custom
        methods.
        """
        self._request_manager.quit()
        self._executor.shutdown()
        self._server_sock.close()

    def on_received_request(self, request):
        """Invoked when the RequestManager gets a new request coming from the
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
            self._handle_received_request(request_id, data, method_name)
        except RemotingException as err:
            self.on_exception(err)

    def _handle_received_request(self, request_id, data, method_name):
        close_request = method_name == str(protocol.Method.CLOSE)
        if close_request and self._close_expected:
            if request_id != '0':
                raise RemotingException("Unexpected id found while parsing"
                                        " a {} request".format(method_name))
            close_data = protocol.read_close(data)
            close_data.setdefault(protocol.KEY_CLOSE_REASON, None)
            closed_reason = close_data[protocol.KEY_CLOSE_REASON]
            if closed_reason is not None:
                self._log.info("Close requested by the counterpart with "
                                        "reason: %s", closed_reason)
            self.close()
            return
        self._handle_request(request_id, data, method_name)

    def _send_reply(self, request_id, response):
        self._log.debug("Sending reply for request: %s", request_id)
        self._request_manager.send_reply(request_id, response)

    def on_ioexception(self, ioexception):
        """Called by the Remote Server upon a read or write operation failure.

        See documentation from the ExceptionHandler.handle_io exception method
        for further details.
        """
        if self._exception_handler is not None:
            self._log.info("Caught exception: %s, notifying the "
                           "application...", str(ioexception))
            # Enable default handling in case the exception handler
            # returns False.
            if not self._exception_handler.handle_ioexception(ioexception):
                return False

        return self._handle_ioexception(ioexception)

    def on_exception(self, exception):
        """Called by the Remote Server upon an unexpected error.

        See documentation from the ExceptionHandler.handle_exception method for
        further details.
        """
        if self._exception_handler is not None:
            self._log.info("Caught exception: %s, notifying the "
                           "application...", str(exception))
            # Enable default handling in case the exception handler
            # returns False.
            if not self._exception_handler.handle_exception(exception):
                return False

        return self._handle_exception(exception)

    @abstractmethod
    def _handle_ioexception(self, ioexception):
        os._exit(1)
        return False

    def _handle_exception(self, exception):
        return False

    @abstractmethod
    def _on_request_manager_started(self):
        """Hook method to notify the subclass that the Request Manager has
        been started.

        This method is intended to be overridden by subclasses.
        """

    @abstractmethod
    def _handle_request(self, request_id, data, method_name):
        """Intended to be overridden by subclasses, invoked for handling the
        received request, already splitted into the supplied parameters.
        """

    def _send_remote_credentials(self):
        """Invoked for sending the remote credentials to the Proxy Adapter.
        """

        unsolicited_message = protocol.write_credentials(self.remote_user,
                                                         self.remote_password)
        self._send_reply("1", unsolicited_message)


class MetadataProviderServer(Server):
    """A Remote Server object which can run a Remote Metadata Adapter and
    connect it to the Proxy Adapter running on Lightstreamer Server.
    Note that since Server version 7.4 the Proxy Adapter also supports
    connection inversion, that is, it can be configured to listen for
    a connection issued by the Remote Server. This option is currently
    not supported by this library.

    The object should be provided with a MetadataProvider instance and with
    suitable initialization parameters,
    then activated through :meth:`MetadataProviderServer.start` and finally
    disposed through :meth:`Server.close`.
    Further reuse of the same instance is not supported.

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

    def __init__(self, adapter, address, name=None, keep_alive=None,
                 thread_pool_size=0, ssl_context=None):
        """Creates a server with the supplied configuration parameters. The
        :meth:`lightstreamer_adapter.interfaces.metadata.MetadataProvider.initialize`
        method will be invoked only upon a Proxy Adapter request.

        :param lightstreamer_adapter.interfaces.metadata.MetadataProvider \
        adapter: the Remote Metadata Adapter instance to be run.
        :param tuple address: the address of the Proxy Adapter supplied as a
         2-tuple ``(host, request_reply_port)`` where:

         * host: a string representing the hostname or the IP address
         * request_reply_port: an int representing the "request/reply" port
        :param str name: the name associated to the Server instance.
        :param float keep_alive: the keepalive interval expressed in seconds
         (or fractions)
        :param int thread_pool_size: the thread pool size
        :param SSLContext ssl_context: the SSL context to be used in the case
         of encrypted communications with the Proxy Adapter
        :raises TypeError: if the supplied Remote Adapter is not an instance of
         a subclass of
         :class:`lightstreamer_adapter.interfaces.metadata.MetadataProvider`.
        """
        super(MetadataProviderServer, self).__init__(address, name, keep_alive,
                                                     thread_pool_size,
                                                     ssl_context)
        if not isinstance(adapter, MetadataProvider):
            raise TypeError("The provided adapter is not a subclass of "
                            "lightstreamer_adapter.interfaces."
                            "MetadataProvider")
        self._config_file = None
        self._params = None
        self._adapter = adapter
        self.init_expected = True

    def getSupportedVersion(self, proxy_version, max_version):
        # protocol versions up to 1.9.0 identify an old Server version
        # which doesn't support single connection for Data Adapters;
        # since this does not affect Metadata Adapters, we can accept
        if proxy_version in ('1.8.0', '1.8.2'):
            self._log.info("Received Proxy Adapter protocol version as %s "
                           "for %s: accepted downgrade.", proxy_version,
                           self.name)
            return proxy_version
        elif proxy_version == max_version:
            self._log.info("Received Proxy Adapter protocol version as %s "
                           "for %s: versions match", proxy_version,
                           self.name)
            return proxy_version
        else:
            self._log.info("Received Proxy Adapter protocol version as %s "
                           "for %s: requesting %s", proxy_version,
                           self.name, max_version)
            return max_version

    def _on_request_manager_started(self):
        """Invoked to notify this subclass that the Request Manager has been
        started.

        This class has a void implementation.
        """

    def _handle_request(self, request_id, data, method_name):
        init_request = method_name == str(meta_protocol.Method.MPI)
        if init_request and not self.init_expected:
            raise RemotingException("Unexpected late {} request"
                                    .format(str(meta_protocol.Method.MPI)))
        if not init_request and self.init_expected:
            raise RemotingException("Unexpected request {} while waiting for "
                                    "{} request"
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
        # function to be executed through the executor.
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
        return self._on_init(meta_protocol, self._params, self._config_file,
                             data, self._adapter)

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
                    METADATA_LOGGER.warning("None or empty field list from "
                                            "get_items for group '%s'", group)
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
                    METADATA_LOGGER.warning("None or empty field list from "
                                            "get_schema for schema '%s' in "
                                            "group '%s'", schema, group)
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
        METADATA_LOGGER.fatal("Exception caught while reading/writing from/to"
                              " network: <%s>, aborting...", str(ioexception))
        return super(MetadataProviderServer, self)._handle_ioexception(ioexception)

    def _handle_exception(self, exception):
        traceback.print_exc()
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

        The requests are handled through a ThreadPoolExecutor. Since Python
        3.9, this requires that the main thread is kept active until close
        is invoked.

        :raises \
        lightstreamer_adapter.interfaces.metadata.MetadataProviderError: If an
         error occurred in the initialization phase. The adapter was not
         started.
        """
        METADATA_LOGGER.info("Managing Metadata Adapter %s with a thread pool"
                             " size of %d", self.name, self.thread_pool_size)
        try:
            super(MetadataProviderServer, self).start()
        except (TypeError, OSError) as err:
            raise MetadataProviderError("Caught an error during the "
                                        "initialization phase") from err


class DataProviderServer(Server):
    """A Remote Server object which can run a Remote Data Adapter and connect
    it to the Proxy Adapter running on Lightstreamer Server.
    Note that since Server version 7.4 the Proxy Adapter also supports
    connection inversion, that is, it can be configured to listen for
    a connection issued by the Remote Server. This option is currently
    not supported by this library.

    The object should be provided with a DataProvider instance and with
    suitable initialization parameters,
    then activated through :meth:`DataProviderServer.start()`
    and finally disposed through :meth:`Server.close()`.
    Further reuse of the same instance is not supported.

    By default, the invocations to the Data Adapter methods will be done in
    a limited thread pool with a size determined by the number of detected cpu
    cores. The size can be specified through the provided
    ``thread_pool_size`` parameter. A size of 1 enforces strictly sequential
    invocations and can be used if parallelization of the calls is not
    supported by the Data Adapter. A value of 0, negative or ``None`` also
    implies the default behaviour as stated above.

    Note that :meth:`.subscribe` and :meth:`.unsubscribe` invocations for the
    same item are always guaranteed to be sequentialized in the right way,
    although they may not occur in the same thread.
    """

    def __init__(self, adapter, address, name=None, keep_alive=None,
                 thread_pool_size=0, ssl_context=None):
        """Creates a server with the supplied configuration parameters. The
        initialize method of the Remote Adapter will be invoked only upon a
        Proxy Adapter request.

        :param lightstreamer_adapter.interfaces.data.DataProvider adapter: The
         Remote Adapter instance to be run.
        :param tuple address: the address of the Proxy Data Adapter supplied
         as a 2-tuple ``(host, request_reply_port)`` where:

         * host: a string representing the hostname or the IP address
         * request_reply_port: an int representing the "request/reply" port
        :param str name: the name associated to the Server instance.
        :param float keep_alive: the keepalive interval expressed in seconds
         (or fractions)
        :param int thread_pool_size: the thread pool size
        :param SSLContext ssl_context: the SSL context to be used in the
         case of encrypted communications with the Proxy Adapter
        :raises TypeError: if the supplied Remote Adapter is not an instance of
         a subclass of
         :class:`lightstreamer_adapter.interfaces.data.DataProvider`.
        """
        super(DataProviderServer, self).__init__((address[0], address[1]),
                                                 name,
                                                 keep_alive,
                                                 thread_pool_size,
                                                 ssl_context)

        if not isinstance(adapter, DataProvider):
            raise TypeError("The provided adapter is not a subclass of "
                            "lightstreamer_adapter.interfaces.DataProvider")
        self._config_file = None
        self._params = None
        self._adapter = adapter
        self._subscription_mgr = SubscriptionManager(self._executor)
        self.init_expected = True
        if len(address) > 2:
            raise TypeError("Address tuple length longer than the expected 2;"
                            " library upgrade without source code alignment?")

    def getSupportedVersion(self, proxy_version, max_version):
        # protocol versions up to 1.9.0 identify an old Server version
        # which doesn't support single connection for Data Adapters;
        # hence we prefer not to accept them, because, otherwise,
        # the connection would fail anyway
        if proxy_version == max_version:
            self._log.info("Received Proxy Adapter protocol version as %s "
                           "for %s: versions match but refused "
                           "for Proxy Adapter incompatibility.",
                           proxy_version, self.name)
            raise Exception("Incompatible Proxy Adapter for protocol "
                            "version: {}".format(proxy_version))
        elif proxy_version.startswith('1.8.'):
            self._log.info("Received Proxy Adapter protocol version as %s "
                           "for %s: compatible but refused for Proxy Adapter "
                           "incompatibility.", proxy_version, self.name)
            raise Exception("Incompatible Proxy Adapter for protocol "
                            "version: {}".format(proxy_version))
        elif proxy_version == '1.9.0':
            self._log.info("Received Proxy Adapter protocol version as %s "
                           "for %s: downgrade possible but refused "
                           "for Proxy Adapter incompatibility.",
                           proxy_version, self.name)
            raise Exception("Incompatible Proxy Adapter for protocol "
                            "version: {}".format(proxy_version))
        else:
            self._log.info("Received Proxy Adapter protocol version as %s "
                           "for %s: requesting %s", proxy_version,
                           self.name, max_version)
            return max_version

    def _handle_request(self, request_id, data, method_name):
        init_request = method_name == str(data_protocol.Method.DPI)
        if init_request and not self.init_expected:
            raise RemotingException("Unexpected late {} request"
                                    .format(str(data_protocol.Method.DPI)))
        if not init_request and self.init_expected:
            raise RemotingException("Unexpected request {} while waiting for "
                                    "{} request"
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

    def _on_request_manager_started(self):
        """Invoked to notify this subclass that the Request Manager has been
        started.

        This method takes care of enabling the communication
        from the Remote Data Adapter to the Proxy Adapter, in order to send
        data over the "notifications" subchannel.
        This may require creating an additional socket, if the backward
        compatibility configuration has been leveraged.
        """

        notification_log = logging.getLogger("lightstreamer-adapter."
                                             "requestreply.notifications")

        # we can send notifications through the inherited reply sender;
        # the only difference is that the notifications
        # should be logged by a dedicated logger
        self._request_manager._reply_sender._notification_log = notification_log

    def _on_dpi(self, data):
        return self._on_init(data_protocol, self._params, self._config_file,
                             data, self._adapter, True)

    def _on_sub(self, request_id, data):
        item_name = data_protocol.read_sub(data)

        def do_task():
            DATA_LOGGER.debug("Processing SUB request: %s", request_id)
            success = False
            try:
                snpt_available = self._adapter.issnapshot_available(item_name)
                if snpt_available is False:
                    # we have to send an empty snapshot;
                    # this should be done before letting the Data Adapter start the subscription,
                    # to ensure that the snapshot precedes the real time updates;
                    # note that it also precedes the reply to the subscribe request,
                    # hence it may even precede an unsuccessful reply,
                    # but this is not forbidden by the ARI protocol
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
        self._request_manager.send_notify(ntfy)

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
            DATA_LOGGER.warning("Unexpected update for item_name %s",
                                item_name)

    def end_of_snapshot(self, item_name):
        request_id = self._subscription_mgr.get_active_item(item_name)
        if request_id:
            try:
                res = data_protocol.write_eos(item_name, request_id)
                self._send_notify(res)
            except RemotingException as err:
                self.on_exception(err)
        else:
            DATA_LOGGER.warning("Unexpected end_of_snapshot notify for "
                                "item_name %s", item_name)

    def clear_snapshot(self, item_name):
        request_id = self._subscription_mgr.get_active_item(item_name)
        if request_id:
            try:
                res = data_protocol.write_cls(item_name, request_id)
                self._send_notify(res)
            except RemotingException as err:
                self.on_exception(err)
        else:
            DATA_LOGGER.warning("Unexpected clear_snapshot for item_name %s",
                                item_name)

    def failure(self, exception):
        try:
            res = data_protocol.write_failure(exception)
            self._send_notify(res)
        except RemotingException as err:
            self.on_exception(err)

    def _handle_ioexception(self, ioexception):
        DATA_LOGGER.fatal("Exception caught while reading/writing from/to "
                          "network: <%s>, aborting...", str(ioexception))
        return super(DataProviderServer, self)._handle_ioexception(ioexception)

    def _handle_exception(self, exception):
        traceback.print_exc()
        DATA_LOGGER.error("Caught exception: %s, trying to notify a "
                          "failure...", str(exception))
        try:
            res = data_protocol.write_failure(exception)
            self._send_notify(res)
        except RemotingException:
            DATA_LOGGER.exception("Caught second-level exception while trying"
                                  " to notify a first-level exception")
        return False

    def start(self):
        """Starts the Remote Data Adapter. A connection to the Proxy Adapter is
        performed (as soon as one is available). Then, requests issued by the
        Proxy Adapter are received and forwarded to the Remote Adapter.

        The requests are handled through a ThreadPoolExecutor. Since Python
        3.9, this requires that the main thread is kept active until close
        is invoked.

        :raises lightstreamer_adapter.interfaces.data.DataProviderError: If an
         error occurred in the initialization phase. The adapter was not
         started.
        """
        DATA_LOGGER.info("Managing Data Adapter %s with a thread pool size of"
                         " %d", self.name, self.thread_pool_size)
        try:
            super(DataProviderServer, self).start()
        except (TypeError, OSError) as err:
            raise DataProviderError("Caught an error during the "
                                    "initialization phase") from err


class ExceptionHandler(metaclass=ABCMeta):
    """An abstract class meant to to be implemented in order to provide a
    Remote Server instance with a custom handler for error conditions occurring
    on the     Remote Server.

    Note that multiple redundant invocations on the same Remote Server instance
    are possible.
    """

    def __init__(self):
        pass

    def handle_ioexception(self, exception):
        """Called by the Remote Server upon a read or write operation failure.
        This may mean that the connection to Lightstreamer Server is lost; in
        any way, after this error, the correct operation of this Remote Server
        operation is compromised.
        This can be the signal of a normal termination of Lightstreamer Server.
        If this is not the case, then this Remote Server should be closed and a
        new one should be created and initialized. This may mean closing and
        restarting the process or just creating a new instance, depending on
        the implementation choice. This will be detected by the Proxy Adapter,
        which will react accordingly.

        The default handling just terminates the process.

        :param Exception exception: An Exception showing the cause of the
         problem.
        :return bool: ``True`` to enable the default handling, false to
         suppress it.
        """

    def handle_exception(self, exception):
        """Called by the Remote Server upon an unexpected error. After this
        error, the correct operation of this Remote Server instance is
        compromised.
        If this is the case, then this Remote Server instance should be closed
        and a new one should be created and initialized. This may mean closing
        and restarting the process or just creating a new instance, depending
        on the implementation choice. This will be detected by the Proxy
        Adapter, which will react accordingly.

        The default handling, in case of a Remote Data Adapter, issues an
        asynchronous failure notification to the Proxy Adapter.
        In case of a Remote Metadata Adapter, the default handling ignores the
        notification; however, as a consequence of the Remote Protocol being
        broken, the Proxy Adapter may return exceptions against one or more
        specific requests by Lightstreamer Kernel.

        :param Exception exception: An Exception showing the cause of the
         problem.
        :return: ``True`` to enable the default handling, false to
         suppress it.
        :rtype: bool
        """
