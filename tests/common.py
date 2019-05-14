import socket
import threading
import logging
import queue
import unittest
from enum import Enum
from lightstreamer_adapter.server import ExceptionHandler

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger("lightstreamer-test_server")


class LightstreamerServerSimulator():

    def __init__(self, req_reply_adr, notify_adr, enable_notify=False):
        self.main_thread = None
        # Request-Reply Socket
        self._rr_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._rr_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._rr_sock.bind(req_reply_adr)
        self._rr_sock.listen(1)
        self._rr_client_socket = None
        self._ntfy_sock, self._ntfy_client_sock = None, None
        if enable_notify is True:
            self._ntfy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._ntfy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,
                                       1)
            self._ntfy_sock.bind(notify_adr)
            self._ntfy_sock.listen(1)

    def _accept_connections(self):
        LOG.info("Listening at %s", self._rr_sock.getsockname())
        self._rr_client_socket, rr_client_addr = self._rr_sock.accept()
        LOG.info("Accepted connection from %s on %s", rr_client_addr,
                 self._rr_sock.getsockname())
        LOG.info("Listener at %s closed", self._rr_sock.getsockname())
        self._rr_sock.close()

        if self._ntfy_sock is not None:
            LOG.info("Listening at %s", self._ntfy_sock.getsockname())
            self._ntfy_client_sock, ntfy_client_addr = self._ntfy_sock.accept()
            LOG.info("Accepted a connection from %s on %s", ntfy_client_addr,
                     self._ntfy_sock.getsockname())
            LOG.info("Listener at %s closed", self._ntfy_sock.getsockname())
            self._ntfy_sock.close()

    def set_rr_socket_timeout(self, timeout):
        self._rr_client_socket.settimeout(timeout)

    def set_notify_socket_timeout(self, timeout):
        self._ntfy_client_sock.settimeout(timeout)

    def start(self):
        self.main_thread = threading.Thread(target=self._accept_connections,
                                            name="Simple Server Thread")
        self.main_thread.start()
        LOG.info("Started accepting new connections")

    def send_request(self, request):
        protocol_request = request + '\r\n'
        self._rr_client_socket.sendall(bytes(protocol_request, "utf-8"))
        LOG.info("Sent request: %s", protocol_request)

    def receive_replies(self, skip_keepalive):
        reply = self._read_from_socket(self._rr_client_socket, skip_keepalive,
                                       split=False)
        LOG.info("Received reply: %s", reply)
        return reply

    def receive_notifications(self, skip_keepalive):
        notify = self._read_from_socket(self._ntfy_client_sock,
                                        skip_keepalive, split=True)
        LOG.info("Received notify: %s", notify)
        return notify

    def _read_from_socket(self, sock, skip_keepalive, split):
        buffer = ''
        notifications = []
        while True:
            LOG.debug("Reading from socket...")
            more = sock.recv(1024)
            LOG.debug("Received %d bytes of data", len(more))
            if not more:
                raise EOFError('Socket connection broken')
            buffer += more.decode()
            tokens = buffer.splitlines(keepends=True)
            for notify in tokens:
                if notify.endswith('\r\n') and (not skip_keepalive or
                                                notify.strip() != 'KEEPALIVE'):
                    if split is True:
                        notify = '|'.join(notify.split("|")[1:])
                    notifications.append(notify.rstrip())
                    buffer = ''
                else:
                    buffer = notify
            if not buffer:
                break
        return notifications

    def stop(self):
        self._rr_client_socket.close()
        if self._ntfy_client_sock is not None:
            self._ntfy_client_sock.shutdown(socket.SHUT_WR)


class RemoteAdapterBase(unittest.TestCase):

    _HOST, _REQ_REPLY_PORT, _NOTIFY_PORT = 'localhost', 6662, 6663
    _REQUEST_REPLY_ADDRESS = (_HOST, _REQ_REPLY_PORT)
    _NOTIFY_ADDRESS = (_HOST, _NOTIFY_PORT)
    PROXY_METADATA_ADAPTER_ADDRESS = (_HOST, _REQ_REPLY_PORT)
    PROXY_DATA_ADAPTER_ADDRESS = (_HOST, _REQ_REPLY_PORT, _NOTIFY_PORT)

    def setUp(self):
        LOG.info("\n\nStarting new test...")
        self._remote_server = None
        self._exception_handler = None
        # Configures and starts the Lightstreamer Server simulator
        self._ls_server = LightstreamerServerSimulator(
            RemoteAdapterBase._REQUEST_REPLY_ADDRESS,
            RemoteAdapterBase._NOTIFY_ADDRESS,
            self.is_enable_notify())
        self._ls_server.start()
        self.on_setup()
        LOG.info("setUp completed\n\n")

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
    def remote_server(self):
        return self._remote_server

    def tearDown(self):
        if self._remote_server is not None:
            self._remote_server.close()
            if self._remote_server._exception_handler is not None:
                self._remote_server._exception_handler.join()

        # Stops the Lightstreamer Server Simulator
        self._ls_server.stop()
        LOG.info("Test completed")

    def send_request(self, request, skip_reply=False):
        self._ls_server.send_request(request)
        if skip_reply:
            self._ls_server.receive_replies(skip_keepalive=True)

    def receive_replies(self):
        return self._ls_server.receive_replies(skip_keepalive=True)

    def receive_notifications(self):
        return self._ls_server.receive_notifications(skip_keepalive=True)

    def assert_reply(self, expected=None, timeout=0.2, skip_keepalive=True):
        self._ls_server.set_rr_socket_timeout(timeout)
        reply = self._ls_server.receive_replies(skip_keepalive)
        self.assertEqual(len(reply), 1)
        self.assertEqual(expected, reply[0])

    def assert_not_reply(self, not_expected=None, timeout=0.2,
                         skip_keepalive=True):
        self._ls_server.set_rr_socket_timeout(timeout)
        reply = self._ls_server.receive_replies(skip_keepalive)
        self.assertEqual(len(reply), 1)
        self.assertNotEqual(not_expected, reply[0])

    def assert_notify(self, expected=None):
        self._ls_server.set_notify_socket_timeout(3.5)
        notifications = self._ls_server.receive_notifications(True)
        self.assertEqual(len(notifications), 1)
        self.assertEqual(expected, notifications[0])

    def assert_caught_exception(self, msg):
        self.assertEqual(msg, self._remote_server._exception_handler.get())


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
