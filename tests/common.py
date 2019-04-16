import socket
import threading
import logging
import unittest
from enum import Enum
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger("lightstreamer-test_server")


class LightstreamerServerSimulator():

    def __init__(self, req_reply_adr, notify_adr=None):
        # Request-Reply Socket
        self._rr_sock = socket.socket(socket.AF_INET,
                                      socket.SOCK_STREAM)
        self._rr_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._rr_sock.bind(req_reply_adr)
        self._rr_sock.listen(1)
        self._rr_client_socket = None
        self._notify_sock, self._ntfy_client_socket = None, None
        if notify_adr is not None:
            self._notify_sock = socket.socket(socket.AF_INET,
                                              socket.SOCK_STREAM)
            self._notify_sock.setsockopt(socket.SOL_SOCKET,
                                         socket.SO_REUSEADDR, 1)
            self._notify_sock.bind(notify_adr)
            self._notify_sock.listen(1)

    def _accept_connections(self):
        log.info("Listening at %s", self._rr_sock.getsockname())
        self._rr_client_socket, rr_client_addr = self._rr_sock.accept()
        log.info("Accepted a connection from %s on %s", rr_client_addr,
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

    def retrieve_connection(self):
        log.debug("Retrieving connection...")
        self.main_thread.join()
        if self._rr_client_socket is not None:
            log.info("Connection retrieved")
            return self._rr_client_socket

        log.error("No connection available!")

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
        log.info("Received notify: {}".format(notify))
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

    HOST, REQ_REPLY_PORT, NOTIFY_PORT = 'localhost', 6662, 6663
    REQ_REPLY_ADDRESS = (HOST, REQ_REPLY_PORT)
    NOTIFY_ADDRESS = (HOST, NOTIFY_PORT)

    def setUp(self):
        log.info("\n\nStarting new test...")
        self._remote_adapter = None
        # Configures and starts the Lightstreamer Server simulator
        self._ls_server = LightstreamerServerSimulator(
                                                  self.get_req_reply_address(),
                                                  self.get_notify_address())
        self._ls_server.start()
        self.on_setup()
        log.info("setUp completed\n\n")

    def on_setup(self):
        pass

    def on_teardown(self):
        pass

    def launch_remote_adapter(self, remote_adapter):
        self._remote_adapter = remote_adapter
        self._remote_adapter.start()

    @property
    def remote_adapter(self):
        return self._remote_adapter

    def get_req_reply_address(self):
        return RemoteAdapterBase.REQ_REPLY_ADDRESS

    def get_notify_address(self):
        return None

    def tearDown(self):
        if self._remote_adapter is not None:
            self._remote_adapter.close()

            # Invoke the hook to notify that the Remote Adapter is closing
            self.on_teardown()

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


class KeepaliveConstants(Enum):
    DEFAULT = 10.0
    MIN = 1.0
    STRICTER = 1.0
