import socket
import threading
import logging
import queue
import unittest
from enum import Enum
from lightstreamer_adapter.server import ExceptionHandler
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger("lightstreamer-test_server")


class LightstreamerServerSimulator():

    def __init__(self, req_reply_adr, notify_adr, enable_notify=False):
        self.main_thread = None
        # Request-Reply Socket
        self._rr_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._rr_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._rr_sock.bind(req_reply_adr)
        self._rr_sock.listen(1)
        self._rr_client_socket = None
        self._notify_sock, self._ntfy_client_socket = None, None
        if enable_notify is True:
            self._notify_sock = socket.socket(socket.AF_INET,
                                              socket.SOCK_STREAM)
            self._notify_sock.setsockopt(socket.SOL_SOCKET,
                                         socket.SO_REUSEADDR, 1)
            self._notify_sock.bind(notify_adr)
            self._notify_sock.listen(1)

    def _accept_connections(self):
        log.info("Listening at %s", self._rr_sock.getsockname())
        self._rr_client_socket, rr_client_addr = self._rr_sock.accept()
        log.info("Accepted connection from %s on %s", rr_client_addr,
                 self._rr_sock.getsockname())
        log.info("Listener at %s closed", self._rr_sock.getsockname())
        self._rr_sock.close()

        if self._notify_sock is not None:
            log.info("Listening at %s", self._notify_sock.getsockname())
            self._ntfy_client_socket, ntfy_client_addr = self._notify_sock.accept()
            log.info("Accepted a connection from %s on %s", ntfy_client_addr,
                             self._notify_sock.getsockname())
            log.info("Listener at %s closed", self._notify_sock.getsockname())
            self._notify_sock.close()

    def set_rr_socket_timeout(self, timeout):
        self._rr_client_socket.settimeout(timeout)

    def start(self):
        self.main_thread = threading.Thread(target=self._accept_connections,
                                            name="Simple Server Thread")
        self.main_thread.start()
        log.info("Started accepting new connections")

    def send_request(self, request):
        protocol_request = request + '\r\n'
        self._rr_client_socket.sendall(bytes(protocol_request, "utf-8"))
        log.info("Sent request: %s", protocol_request)

    def receive_reply(self):
        reply = self._get_reply(self._rr_client_socket)
        log.info("Received reply: %s", reply)
        return reply

    def receive_notify(self, timeout=3.5):
        self._ntfy_client_socket.settimeout(timeout)
        notify = self._get_notifications(self._ntfy_client_socket)
        log.info("Received notify: %s", notify)
        return notify

    def _get_reply(self, sock):
        data = b''
        while True:
            log.debug("Reading from request-reply socket...")
            more = sock.recv(1024)
            log.debug("Received %d bytes of reply data", len(more))
            if not more:
                raise EOFError('Socket connection broken')
            data += more
            if data.endswith(b'\n'):
                break
        return data.decode()

    def _get_notifications(self, sock):
        buffer = ''
        notifications = []
        while True:
            log.debug("Reading from notification socket...")
            more = sock.recv(1024)
            log.debug("Received %d bytes of notification data", len(more))
            if not more:
                raise EOFError('Socket connection broken')
            buffer += more.decode()
            tokens = buffer.splitlines(keepends=True)
            for notify in tokens:
                if (notify.endswith('\r\n') and notify.rstrip() != 'KEEPALIVE'):
                    stripped = '|'.join(notify.split("|")[1:]).rstrip()
                    notifications.append(stripped)
                    buffer = ''
                else:
                    buffer = notify
            if not buffer:
                break
        return notifications

    def stop(self):
        self._rr_client_socket.close()
        if self._ntfy_client_socket is not None:
            self._ntfy_client_socket.shutdown(socket.SHUT_WR)


class RemoteAdapterBase(unittest.TestCase):

    _HOST, _REQ_REPLY_PORT, _NOTIFY_PORT = 'localhost', 6662, 6663
    _REQUEST_REPLY_ADDRESS = (_HOST, _REQ_REPLY_PORT)
    _NOTIFY_ADDRESS = (_HOST, _NOTIFY_PORT)
    PROXY_METADATA_ADAPTER_ADDRESS = (_HOST, _REQ_REPLY_PORT)
    PROXY_DATA_ADAPTER_ADDRESS = (_HOST, _REQ_REPLY_PORT, _NOTIFY_PORT)

    def setUp(self):
        log.info("\n\nStarting new test...")
        self._remote_server = None
        self._exception_handler = None
        # Configures and starts the Lightstreamer Server simulator
        self._ls_server = LightstreamerServerSimulator(
                                                  RemoteAdapterBase._REQUEST_REPLY_ADDRESS,
                                                  RemoteAdapterBase._NOTIFY_ADDRESS,
                                                  self.is_enable_notify())
        self._ls_server.start()
        self.on_setup()
        log.info("setUp completed\n\n")

    def is_enable_notify(self):
        return False

    def on_setup(self):
        pass

    def on_teardown(self):
        pass

    def launch_remote_server(self, remote_server, set_exception_handler=False):
        self._remote_server = remote_server
        if set_exception_handler is True:
            self._remote_server.set_exception_handler(MyExceptionHandler())
        self._remote_server.start()

    @property
    def exception_handler(self):
        return self._remote_server._exception_handler

    @property
    def remote_server(self):
        return self._remote_server

    def tearDown(self):
        if self._remote_server is not None:
            self._remote_server.close()
        if self.exception_handler is not None:
            self.exception_handler.join()

        # Stops the Lightstreamer Server Simulator
        self._ls_server.stop()
        log.info("Test completed")

    def send_request(self, request, skip_reply=False):
        self._ls_server.send_request(request)
        if skip_reply:
            self._ls_server.receive_reply()

    def receive_notify(self):
        return self._ls_server.receive_notify()

    def assert_reply(self, expected=None, timeout=0.2):
        self._ls_server.set_rr_socket_timeout(timeout)
        reply = self._ls_server.receive_reply()
        self.assertEqual(expected + '\r\n', reply)

    def assert_not_reply(self, not_expected=None, timeout=0.2):
        self._ls_server.set_rr_socket_timeout(timeout)
        reply = self._ls_server.receive_reply()
        self.assertNotEqual(not_expected + '\r\n', reply)

    def assert_notify(self, expected=None):
        notifications = self.receive_notify()
        self.assertEqual(len(notifications), 1)
        self.assertEqual(expected, notifications[0])

    def assert_caught_exception(self, msg):
        self.assertEqual(msg, self.exception_handler.get())


class MyExceptionHandler(ExceptionHandler):

    def __init__(self):
        super(MyExceptionHandler, self).__init__()
        self._caught_exception_queue = queue.Queue()

    def handle_io_exception(self, ioexception):
        print("MyExceptionHandler-> Got IO Exception {}".format(ioexception))
        return False

    def handle_exception(self, exception):
        print("MyExceptionHandler-> Caught exception: {}"
              .format(str(exception)))
        self._caught_exception_queue.put(str(exception))
        self._caught_exception_queue.task_done()
        return False

    def get(self):
        return self._caught_exception_queue.get(timeout=0.3)

    def join(self):
        self._caught_exception_queue.join()


class KeepaliveConstants(Enum):
    DEFAULT = 10.0
    MIN = 1.0
    STRICTER = 1.0
