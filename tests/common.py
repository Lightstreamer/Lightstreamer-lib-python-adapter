import socket
import threading
import logging
import unittest

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger("lightstreamer-test_server")


class LightstreamerServerSimulator(unittest.TestCase):

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
        log.info("Listening at {}" .format(self._rr_sock.getsockname()))
        self._rr_client_socket, rr_client_addr = self._rr_sock.accept()
        log.info("Accepted a connection from {} on {}"
                 .format(rr_client_addr, self._rr_sock.getsockname()))
        log.info("Listener at {} closed"
                 .format(self._rr_sock.getsockname()))
        self._rr_sock.close()

        if self._notify_sock is not None:
            log.info("Listening at {}".format(self._notify_sock.getsockname()))
            socket_pair = self._notify_sock.accept()
            self._ntfy_client_socket = socket_pair[0]
            ntfy_client_addr = socket_pair[1]
            log.info("Accepted a connection from {} on {}"
                     .format(ntfy_client_addr,
                             self._notify_sock.getsockname()))
            log.info("Listener at {} closed"
                     .format(self._notify_sock.getsockname()))
            self._notify_sock.close()

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
        self._rr_client_socket.sendall(bytes(request + '\n', "utf-8"))
        log.info("Sent request: {}".format(request))

    def receive_reply(self):
        reply = self._get_data(self._rr_client_socket)
        log.info("Received reply: {}".format(reply))
        return reply

    def receive_notify(self):
        notify = self._get_data(self._ntfy_client_socket)
        log.info("Received notify: {}".format(notify))
        return notify

    def _get_data(self, sock):
        data = b''
        while True:
            log.debug("Reading from socket...")
            more = sock.recv(1024)
            log.debug("Received {} test_data".format(len(more)))
            if not more:
                raise EOFError('Socket connection broken')
            data += more
            if data.endswith(b'\n'):
                break
        return data.decode()

    def stop(self):
        self._rr_client_socket.close()
        if self._ntfy_client_socket is not None:
            self._ntfy_client_socket.shutdown(socket.SHUT_WR)


class RemoteAdapterBase(unittest.TestCase):

    HOST, REQ_REPLY_PORT, NOTIFY_PORT = 'localhost', 6662, 6663
    REQ_REPLY_ADDRESS = (HOST, REQ_REPLY_PORT)
    NOTIFY_ADDRESS = (HOST, NOTIFY_PORT)

    def __init__(self, method_name):
        super(RemoteAdapterBase, self).__init__(method_name)

    def setUp(self):
        log.info("\n\nStarting new test...")
        # Configures and starts the Lightstreamer Server simulator
        self._ls_server = LightstreamerServerSimulator(
                                                  self.get_req_reply_address(),
                                                  self.get_notify_address())
        self._ls_server.start()
        # Configures and start the Remote Adapter
        self._remote_adapter = self.on_setup()
        self._remote_adapter.start()
        # Waiting for a new incoming connection (blocking)
        self._rr_client_socket = self._ls_server.retrieve_connection()
        log.info("setUp completed\n\n")

    def on_setup(self):
        pass

    def on_teardown(self, remote_adapter):
        pass

    @property
    def remote_adapter(self):
        return self._remote_adapter

    def get_req_reply_address(self):
        return RemoteAdapterBase.REQ_REPLY_ADDRESS

    def get_notify_address(self):
        return None

    def tearDown(self):
        self._remote_adapter.close()

        # Invoke the hook to notify that the Remote Adapter is closing
        self.on_teardown(self._remote_adapter)

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
        self._ls_server._rr_client_socket.settimeout(timeout)
        reply = self._ls_server.receive_reply()
        self.assertEqual(expected + '\r\n', reply)

    def assert_not_reply(self, not_expected=None, timeout=0.2):
        self._ls_server._rr_client_socket.settimeout(timeout)
        reply = self._ls_server.receive_reply()
        self.assertNotEqual(not_expected + '\r\n', reply)

    def assert_notify(self, expected=None, timeout=0.5):
        self._ls_server._ntfy_client_socket.settimeout(timeout)
        notify = self.receive_notify()
        stripped_notify = '|'.join(notify.split("|")[1:])
        self.assertEqual(expected + '\r\n', stripped_notify)
