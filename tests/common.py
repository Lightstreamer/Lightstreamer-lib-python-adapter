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

    def __init__(self, req_reply_adr):
        self.main_thread = None
        # Request-Reply Socket
        self._rr_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._rr_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._rr_sock.bind(req_reply_adr)
        self._rr_sock.listen(1)
        self._rr_client_socket = None
        self._rr_socket_queue = None

    def _accept_connections(self):
        LOG.info("Listening at %s", self._rr_sock.getsockname())
        self._rr_client_socket, rr_client_addr = self._rr_sock.accept()
        LOG.info("Accepted connection from %s on %s", rr_client_addr,
                 self._rr_sock.getsockname())
        LOG.info("Listener at %s closed", self._rr_sock.getsockname())
        self._rr_socket_queue = queue.Queue()
        self._rr_sock.close()

    def set_rr_socket_timeout(self, timeout):
        self._rr_client_socket.settimeout(timeout)

    def start(self):
        self.main_thread = threading.Thread(target=self._accept_connections,
                                            name="Simple Server Thread")
        self.main_thread.start()
        LOG.info("Started accepting new connections")

    def send_request(self, request):
        protocol_request = request + '\r\n'
        self._rr_client_socket.sendall(bytes(protocol_request, "utf-8"))
        LOG.info("Sent request: %s", protocol_request)

    def receive_reply(self, skip_keepalive):
        reply = self._get_next_message(skip_keepalive)
        LOG.info("Received reply: %s", reply)
        return reply

    def receive_notification(self, skip_keepalive):
        notification = self._get_next_message(skip_keepalive)
        notification = '|'.join(notification.split("|")[1:])
        LOG.info("Received notify: %s", notification)
        return notification

    def skip_messages(self):
        messages = self._get_curr_messages()
        LOG.info("Skipping received messages: %s", messages)

    def _get_curr_messages(self):
        messages = []
        self._rr_client_socket.settimeout(0.2)
        msg = self._get_next_message(skip_keepalive=True)
        self._rr_client_socket.settimeout(5)
        while True:
            messages.append(msg)
            msg = self._get_next_message(skip_keepalive=False)
            if msg == "KEEPALIVE":
                return messages

    def _get_next_message(self, skip_keepalive):
        while True:
            if self._rr_socket_queue.empty():
                self._read_from_socket()
            else:
                msg = self._rr_socket_queue.get_nowait()
                if not skip_keepalive or msg != 'KEEPALIVE':
                    return msg

    def _read_from_socket(self):
        buffer = ''
        while True:
            LOG.debug("Reading from socket...")
            more = self._rr_client_socket.recv(1024)
            LOG.debug("Received %d bytes of data", len(more))
            if not more:
                raise EOFError('Socket connection broken')
            buffer += more.decode()
            tokens = buffer.splitlines(keepends=True)
            for line in tokens:
                if line.endswith('\r\n'):
                    self._rr_socket_queue.put(line.strip())
                    buffer = ''
                else:
                    # it must be an incomplete line
                    # and it must be the last in tokens
                    buffer = line
            if not buffer:
                break

    def stop(self):
        self._rr_client_socket.shutdown(socket.SHUT_WR)


class RemoteAdapterBase(unittest.TestCase):

    _HOST, _REQ_REPLY_PORT = 'localhost', 6662
    _REQUEST_REPLY_ADDRESS = (_HOST, _REQ_REPLY_PORT)
    PROXY_METADATA_ADAPTER_ADDRESS = (_HOST, _REQ_REPLY_PORT)
    PROXY_DATA_ADAPTER_ADDRESS = (_HOST, _REQ_REPLY_PORT)

    def setUp(self):
        LOG.info("\n\n========> Starting new test...")
        LOG.info("setUp for test " + self._testMethodName)
        self._remote_server = None
        self._exception_handler = None
        # Configures and starts the Lightstreamer Server simulator
        self._ls_server = LightstreamerServerSimulator(
            RemoteAdapterBase._REQUEST_REPLY_ADDRESS)
        self._ls_server.start()
        self.on_setup()
        LOG.info("setUp completed\n\n")

    def on_setup(self):
        pass

    def on_teardown(self):
        pass

    def launch_remote_server(self, remote_server, set_exception_handler=False):
        self._remote_server = remote_server
        if set_exception_handler is True:
            self._remote_server.set_exception_handler(MyExceptionHandler())
        self._remote_server.start()
        # this opens the sockets
        self._ls_server.main_thread.join()

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

    def send_request(self, request):
        self._ls_server.send_request(request)

    def receive_reply(self):
        self._ls_server.set_rr_socket_timeout(0.2)
        return self._ls_server.receive_reply(skip_keepalive=True)

    def receive_notification(self):
        self._ls_server.set_rr_socket_timeout(0.5)
        return self._ls_server.receive_notification(skip_keepalive=True)

    def skip_messages(self):
        return self._ls_server.skip_messages()

    def assert_reply(self, expected=None, timeout=0.2, skip_keepalive=True):
        self._ls_server.set_rr_socket_timeout(timeout)
        reply = self._ls_server.receive_reply(skip_keepalive)
        self.assertEqual(expected, reply)

    def assert_notify(self, expected=None, timeout=0.5, skip_keepalive=True):
        self._ls_server.set_rr_socket_timeout(timeout)
        notification = self._ls_server.receive_notification(skip_keepalive)
        self.assertEqual(expected, notification)

    def assert_caught_exception(self, msg):
        self.assertEqual(msg, self._remote_server._exception_handler.get())

    def assert_no_caught_exception(self):
        self.assertTrue(self._remote_server._exception_handler.empty())


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

    def empty(self):
        return self._caught_exception_queue.empty()


class KeepaliveConstants(Enum):
    DEFAULT = 10.0
    MIN = 1.0
    STRICTER = 1.0
