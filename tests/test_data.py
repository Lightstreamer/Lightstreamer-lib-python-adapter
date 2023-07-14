import time
import unittest
import logging
import queue
from collections import OrderedDict
from multiprocessing import cpu_count
from lightstreamer_adapter.server import DataProviderServer
from lightstreamer_adapter.interfaces.data import (DataProviderError,
                                                   SubscribeError,
                                                   FailureError,
                                                   DataProvider)

from .common import KeepaliveConstants, RemoteAdapterBase

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

# Specify here the number of your CPU cores
EXPECTED_CPU_CORES = cpu_count()

# Keep aligned with the ARI version currently implemented
basic_version = '1.8.3'
# Temporarily used because it is the only version accepted
accepted_version = '1.9.1'

def assert_credentials_response(remote_adapter):
    remote_adapter.assert_reply("1|RAC|S|enableClosePacket|S|true"
                                "|S|SDK|S|Python+Adapter+SDK")

class DataProviderTestClass(DataProvider):

    def __init__(self, collector):
        self.collector = collector
        self.config_file = None
        self.subscribed = queue.Queue()

    def initialize(self, parameters, config_file=None):
        self.config_file = config_file
        if parameters and "par1" not in parameters:
            if "adapters_conf.id" not in parameters:
                raise DataProviderError("The ID must be supplied")
            if "data_provider.name" not in parameters:
                raise RuntimeError("RuntimeError")

        self.collector['params'] = parameters

    def set_listener(self, event_listener):
        self.listener = event_listener

    def issnapshot_available(self, item_name):
        if "nosnap" in item_name:
            return False
            # this will cause the SDK library to send an EOS upon subscribe

        return True

    def subscribe(self, item_name):
        if item_name == "item-err-1":
            raise SubscribeError("Subscription Error")

        if item_name == "item-err-2":
            raise FailureError("Failure Error")

        if item_name == "item-err-3":
            raise RuntimeError("Error")

        self.subscribed.put(item_name)

    def unsubscribe(self, item_name):
        if item_name == "item-err-4":
            raise SubscribeError("Subscription Error")

        if item_name == "item-err-5":
            raise FailureError("Failure Error")

        if item_name == "item-err-6":
            raise RuntimeError("Error")
        self.collector.update({'itemName': item_name})


class DataProviderServerConstructionTest(unittest.TestCase):

    def test_start_with_error(self):
        server = DataProviderServer(
            DataProviderTestClass({}),
            RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS)
        with self.assertRaises(Exception) as err:
            server.start()

        the_exception = err.exception
        self.assertIsInstance(the_exception, DataProviderError)
        self.assertEqual(str(the_exception),
                         "Caught an error during the initialization phase")

    def test_not_right_adapter(self):
        with self.assertRaises(TypeError) as type_error:
            DataProviderServer({},
                               RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS)

        the_exception = type_error.exception
        self.assertIsInstance(the_exception, TypeError)
        self.assertEqual(str(the_exception),
                         "The provided adapter is not a subclass of "
                         "lightstreamer_adapter.interfaces.DataProvider")

    def test_default_properties(self):
        # Test default properties
        server = DataProviderServer(
            DataProviderTestClass({}),
            RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS)

        self.assertEqual('#', server.name[0])
        self.assertEqual(10, server.keep_alive)
        self.assertEqual(EXPECTED_CPU_CORES, server.thread_pool_size)
        self.assertIsNone(server.remote_user)
        self.assertIsNone(server.remote_password)

    def test_thread_pool_size(self):
        # Test non default properties
        server = DataProviderServer(
            DataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS,
            thread_pool_size=2)

        self.assertEqual(2, server.thread_pool_size)

        server = DataProviderServer(
            DataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS,
            thread_pool_size=0)

        self.assertEqual(EXPECTED_CPU_CORES, server.thread_pool_size)

        server = DataProviderServer(
            DataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS,
            thread_pool_size=-2)

        self.assertEqual(EXPECTED_CPU_CORES, server.thread_pool_size)

        server = DataProviderServer(
            DataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS,
            thread_pool_size=None)

        self.assertEqual(EXPECTED_CPU_CORES, server.thread_pool_size)

    def test_keep_alive_value(self):
        # Test non default properties
        server = DataProviderServer(
            DataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS,
            keep_alive=2)
        self.assertEqual(2, server.keep_alive)

        server = DataProviderServer(
            DataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS,
            keep_alive=0)
        self.assertEqual(0, server.keep_alive)

        server = DataProviderServer(
            DataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS,
            keep_alive=-2)
        self.assertEqual(0, server.keep_alive)

        server = DataProviderServer(
            DataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS,
            keep_alive=None)
        self.assertEqual(10, server.keep_alive)

    def test_unset_remote_credentials(self):
        server = DataProviderServer(
            DataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS)
        self.assertIsNone(server.remote_user)
        self.assertIsNone(server.remote_password)

    def test_remote_credentials(self):
        server = DataProviderServer(
            DataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS)
        server.remote_user = "user"
        server.remote_password = "password"
        self.assertEqual("user", server.remote_user)
        self.assertEqual("password", server.remote_password)


class DataProviderServerInitializationTest(RemoteAdapterBase):

    def __init__(self, method_name):
        super(DataProviderServerInitializationTest, self).__init__(method_name)
        self.collector = {}
        self.adapter = DataProviderTestClass(self.collector)

    def setup_remote_adapter(self, keep_alive=None, config=None, params=None,
                             username=None, password=None):
        remote_server = DataProviderServer(
            adapter=self.adapter,
            address=RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS,
            keep_alive=keep_alive, name="DataProviderTest")
        remote_server.adapter_config = config
        remote_server.adapter_params = params
        remote_server.remote_user = username
        remote_server.remote_password = password
        self.launch_remote_server(remote_server)

    def test_no_kalive_hint_and_no_configured_kalive(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|adapters_conf.id|S|DEMO"
                          "|S|data_provider.name|S|STOCKLIST"
                          "|S|ARI.version|S|" + accepted_version)
        self.assert_reply("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertEqual(KeepaliveConstants.STRICTER.value,
                         self.remote_server.keep_alive)

    def test_no_kalive_hint_and_configured_kalive(self):
        configured_keepalive = 5
        self.setup_remote_adapter(configured_keepalive)
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|adapters_conf.id|S|DEMO"
                          "|S|data_provider.name|S|STOCKLIST"
                          "|S|ARI.version|S|" + accepted_version)
        self.assert_reply("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(configured_keepalive, self.remote_server.keep_alive)

    def test_negative_kalive_hint_and_no_configured_kalive(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|adapters_conf.id|S|DEMO"
                          "|S|data_provider.name|S|STOCKLIST|"
                          "|S|keepalive_hint.millis|S|-510"
                          "|S|ARI.version|S|" + accepted_version)
        self.assert_reply("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(KeepaliveConstants.DEFAULT.value,
                         self.remote_server.keep_alive)

    def test_negative_kalive_hint_and_configured_kalive(self):
        configured_keepalive = 6
        self.setup_remote_adapter(configured_keepalive)
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|adapters_conf.id|S|DEMO"
                          "|S|data_provider.name|S|STOCKLIST|"
                          "|S|keepalive_hint.millis|S|-500"
                          "|S|ARI.version|S|" + accepted_version)
        self.assert_reply("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(configured_keepalive, self.remote_server.keep_alive)

    def test_kalive_hint_lt_default_and_no_configured_kalive(self):
        expected_keepalive = 9
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|adapters_conf.id|S|DEMO"
                          "|S|data_provider.name|S|STOCKLIST"
                          "|S|keepalive_hint.millis|S|9000"
                          "|S|ARI.version|S|" + accepted_version)
        self.assert_reply("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(expected_keepalive, self.remote_server.keep_alive)

    def test_kalive_hint_lt_default_and_min_and_no_configured_kalive(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|adapters_conf.id|S|DEMO"
                          "|S|data_provider.name|S|STOCKLIST"
                          "|S|keepalive_hint.millis|S|500"
                          "|S|ARI.version|S|" + accepted_version)
        self.assert_reply("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(KeepaliveConstants.MIN.value,
                         self.remote_server.keep_alive)

    def test_kalive_hint_gt_default_and_no_configured_klive(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|adapters_conf.id|S|DEMO"
                          "|S|keepalive_hint.millis|S|11000"
                          "|S|data_provider.name|S|STOCKLIST"
                          "|S|ARI.version|S|" + accepted_version)
        self.assert_reply("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(KeepaliveConstants.DEFAULT.value,
                         self.remote_server.keep_alive)

    def test_kalive_lt_configured_klive(self):
        expected_keepalive = 4
        configured_keepalive = 5
        self.setup_remote_adapter(configured_keepalive)
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|adapters_conf.id|S|DEMO"
                          "|S|keepalive_hint.millis|S|4000"
                          "|S|data_provider.name|S|STOCKLIST"
                          "|S|ARI.version|S|" + accepted_version)
        self.assert_reply("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(expected_keepalive, self.remote_server.keep_alive)

    def test_kalive_lt_configured_kalive_and_min(self):
        configured_keepalive = 5
        self.setup_remote_adapter(configured_keepalive)
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|adapters_conf.id|S|DEMO"
                          "|S|keepalive_hint.millis|S|500"
                          "|S|data_provider.name|S|STOCKLIST"
                          "|S|ARI.version|S|" + accepted_version)
        self.assert_reply("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(KeepaliveConstants.MIN.value,
                         self.remote_server.keep_alive)

    def test_remote_credentials_with_user_and_password(self):
        self.setup_remote_adapter(username="remote1", password="fdhjkslghak")
        self.assert_reply("1|RAC|S|user|S|remote1|S|password|S|fdhjkslghak"
                          "|S|enableClosePacket|S|true"
                          "|S|SDK|S|Python+Adapter+SDK")

    def test_remote_credentials_with_user(self):
        self.setup_remote_adapter(username="remote1")
        self.assert_reply("1|RAC|S|user|S|remote1"
                          "|S|enableClosePacket|S|true"
                          "|S|SDK|S|Python+Adapter+SDK")

    def test_remote_credentials_with_password(self):
        self.setup_remote_adapter(password="fdhjkslghak")
        self.assert_reply("1|RAC|S|password|S|fdhjkslghak"
                          "|S|enableClosePacket|S|true"
                          "|S|SDK|S|Python+Adapter+SDK")

    def test_remote_credentials_with_no_credentials(self):
        self.setup_remote_adapter()
        self.assert_reply("1|RAC|S|enableClosePacket|S|true"
                          "|S|SDK|S|Python+Adapter+SDK")


class DataProviderServerTest(RemoteAdapterBase):

    def on_setup(self):
        self.collector = {}
        # Configuring and starting MetadataProviderServer
        self.adapter = DataProviderTestClass(self.collector)
        remote_server = DataProviderServer(
            adapter=self.adapter,
            address=RemoteAdapterBase.PROXY_DATA_ADAPTER_ADDRESS,
            name="DataProviderTest")
        self.launch_remote_server(remote_server)

    def on_teardown(self):
        LOG.info("DataProviderTest completed")

    def do_subscription(self, item_name):
        self.send_request("10000010c3e4d0462|SUB|S|" + item_name)

    def do_subscription_with_request_id(self, request_id, item_name):
        self.send_request(request_id + "|SUB|S|" + item_name)

    def do_subscription_and_skip(self, item_name):
        self.send_request("10000010c3e4d0462|SUB|S|" + item_name)
        self.skip_messages()

    def do_unsubscription(self, item_name):
        self.send_request("10000010c3e4d0463|USB|S|" + item_name)

    def do_init(self):
        self.send_request("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + accepted_version)

    def do_init_and_skip(self):
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + accepted_version)
        self.skip_messages()

    def test_default_keep_alive(self):
        assert_credentials_response(self)
        # Receive a KEEPALIVE message because no requests have been issued
        for _ in range(0, 1):
            start = time.time()
            self.assert_reply(expected="KEEPALIVE", timeout=11.1,
                              skip_keepalive=False)
            end = time.time()
            self.assertGreaterEqual(end - start, 0.99)

    def test_no_keep_alive(self):
        # Initialization with no further configurations leads to a keepalive
        # time of 1 second.
        self.do_init_and_skip()
        # Receive a KEEPALIVE message because no request has been issued
        items = ["item1", "item2", "item3"]
        for item_name in items:
            # Wait for half the KEEPALIVE time
            time.sleep(0.5)
            self.do_subscription(item_name)
            self.assert_reply("10000010c3e4d0462|SUB|V", timeout=0.2, skip_keepalive=False)

        # As no more requests have been issued, a period longer than 1 second
        # must have been elapsed, therefore we expect a KEEPALIVE message
        self.assert_reply("KEEPALIVE", timeout=1.1, skip_keepalive=False)

    def test_init(self):
        assert_credentials_response(self)
        self.do_init()
        self.assert_reply("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertDictEqual({}, self.collector['params'])
        self.assertIsNotNone(self.adapter.listener)

    def test_init_with_adapter_config(self):
        self.remote_server.adapter_config = "config.file"
        self.do_init_and_skip()
        self.assertEqual("config.file", self.adapter.config_file)

    def test_init_with_local_params(self):
        self.remote_server.adapter_params = {"par1": "val1", "par2": "val2"}
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + accepted_version)

        self.assert_reply("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertDictEqual({"par1": "val1",
                              "par2": "val2"},
                             self.collector['params'])

    def test_init_with_remote_params(self):
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|adapters_conf.id|S|DEMO|S|"
                          "data_provider.name|S|STOCKLIST"
                          "|S|ARI.version|S|" + accepted_version)
        self.assert_reply("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertDictEqual({"adapters_conf.id": "DEMO",
                              "data_provider.name": "STOCKLIST"},
                             self.collector['params'])

    def test_init_with_local_and_remote_params(self):
        self.remote_server.adapter_params = {"my_param.name": "my_local_param"}
        request = ("10000010c3e4d0462|DPI|S|adapters_conf.id|S|DEMO|S|"
                   "data_provider.name|S|STOCKLIST"
                   "|S|ARI.version|S|" + accepted_version)
        assert_credentials_response(self)
        self.send_request(request)

        self.assert_reply("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertDictEqual({"adapters_conf.id": "DEMO",
                              "data_provider.name": "STOCKLIST",
                              "my_param.name": "my_local_param"},
                             self.collector['params'])

    def test_init_with_protocol_1_8_0(self):
        self.remote_server.adapter_params = {"data_provider.name":
                                             "my_local_provider"}
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|"
                          "adapters_conf.id|S|DEMO|S|data_provider.name|S|"
                          "STOCKLIST")

        self.assert_reply("10000010c3e4d0462|DPI|E|Incompatible+Proxy+Adapter+"
                          "for+protocol+version%3A+1.8.0")
        self.assertFalse('params' in self.collector)

    def test_init_with_protocol_1_8_1(self):
        self.remote_server.adapter_params = {"data_provider.name":
                                             "my_local_provider"}
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|ARI.version|S|1.8.1|S|"
                          "adapters_conf.id|S|DEMO|S|data_provider.name|S|"
                          "STOCKLIST")

        self.assert_reply("10000010c3e4d0462|DPI|E|Unsupported+reserved+"
                          "protocol+version+number%3A+1.8.1")
        self.assertFalse('params' in self.collector)

    def test_init_with_protocol_1_8_2(self):
        self.remote_server.adapter_params = {"data_provider.name":
                                             "my_local_provider"}
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|ARI.version|S|1.8.2|S|"
                          "adapters_conf.id|S|DEMO|S|data_provider.name|S|"
                          "STOCKLIST")

        self.assert_reply("10000010c3e4d0462|DPI|E|Incompatible+Proxy+Adapter+"
                          "for+protocol+version%3A+1.8.2")
        self.assertFalse('params' in self.collector)

    def test_init_with_protocol_1_8_3(self):
        self.remote_server.adapter_params = {"data_provider.name":
                                             "my_local_provider"}
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|ARI.version|S|1.8.3|S|"
                          "adapters_conf.id|S|DEMO|S|data_provider.name|S|"
                          "STOCKLIST")

        self.assert_reply("10000010c3e4d0462|DPI|E|Incompatible+Proxy+Adapter+"
                          "for+protocol+version%3A+1.8.3")
        self.assertFalse('params' in self.collector)

    def test_init_with_protocol_1_8_4(self):
        self.remote_server.adapter_params = {"data_provider.name":
                                             "my_local_provider"}
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|ARI.version|S|1.8.4|S|"
                          "adapters_conf.id|S|DEMO|S|data_provider.name|S|"
                          "STOCKLIST")

        self.assert_reply("10000010c3e4d0462|DPI|E|Incompatible+Proxy+Adapter+"
                          "for+protocol+version%3A+1.8.4")
        self.assertFalse('params' in self.collector)

    def test_init_with_protocol_1_9_0(self):
        self.remote_server.adapter_params = {"data_provider.name":
                                             "my_local_provider"}
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|ARI.version|S|1.9.0|S|"
                          "adapters_conf.id|S|DEMO|S|data_provider.name|S|"
                          "STOCKLIST")

        self.assert_reply("10000010c3e4d0462|DPI|E|Incompatible+Proxy+Adapter+"
                          "for+protocol+version%3A+1.9.0")
        self.assertFalse('params' in self.collector)

    def test_init_with_protocol_1_9_1(self):
        self.remote_server.adapter_params = {"data_provider.name":
                                             "my_local_provider"}
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|ARI.version|S|1.9.1|S|"
                          "adapters_conf.id|S|DEMO|S|data_provider.name|S|"
                          "STOCKLIST")

        self.assert_reply("10000010c3e4d0462|DPI|S|ARI.version|S|1.8.3")
        self.assertDictEqual({"adapters_conf.id": "DEMO",
                              "data_provider.name": "my_local_provider"},
                             self.collector['params'])

    def test_malformed_init_for_unkown_token_type(self):
        request = ("10000010c3e4d0462|DPI"
                   "|S|ARI.version|S|" + accepted_version +
                   "|H|adapters_conf.id|S|DEMO|S|"
                   "data_provider.name|S|STOCKLIST")
        assert_credentials_response(self)
        self.send_request(request)
        self.assert_notify("FAL|E|Unknown+type+%27H%27+found+while+parsing+"
                           "DPI+request")

    def test_malformed_init_for_invalid_number_of_tokens(self):
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|")
        self.assert_notify("FAL|E|Invalid+number+of+tokens+while+parsing+"
                           "DPI+request")

    def test_malformed_init_for_invalid_number_of_tokens2(self):
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S||")
        self.assert_notify("FAL|E|Invalid+number+of+tokens+while+parsing+"
                           "DPI+request")

    def test_malformed_init_for_invalid_number_of_tokens3(self):
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|  |")
        self.assert_notify("FAL|E|Invalid+number+of+tokens+while+parsing+"
                           "DPI+request")

    def test_malformed_init_for_invalid_number_of_tokens4(self):
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|id|S")
        self.assert_notify("FAL|E|Invalid+number+of+tokens+while+parsing+"
                           "DPI+request")

    def test_init_with_data_provider_exception(self):
        assert_credentials_response(self)
        request = ("10000010c3e4d0462|DPI|S|data_provider.name|S|STOCKLIST"
                   "|S|ARI.version|S|" + accepted_version)
        self.send_request(request)
        self.assert_reply("10000010c3e4d0462|DPI|ED|The+ID+must+be+supplied")

    def test_init_with_generic_exception(self):
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|adapters_conf.id|S|DEMO"
                          "|S|ARI.version|S|" + accepted_version)
        self.assert_reply("10000010c3e4d0462|DPI|E|RuntimeError")

    def test_init_init(self):
        self.do_init_and_skip()
        self.do_init()
        self.assert_notify("FAL|E|Unexpected+late+DPI+request")

    def test_init_miss(self):
        assert_credentials_response(self)
        # Test error when the very first request is not a DPI request
        self.do_subscription('item')
        self.assert_notify("FAL|E|Unexpected+request+SUB+while+waiting+for+DPI"
                           "+request")

    def test_close(self):
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|DPI|S|adapters_conf.id|S|DEMO|S|"
                          "data_provider.name|S|STOCKLIST"
                          "|S|ARI.version|S|" + accepted_version)
        self.assert_reply("10000010c3e4d0462|DPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertDictEqual({"adapters_conf.id": "DEMO",
                              "data_provider.name": "STOCKLIST"},
                             self.collector['params'])

        self.send_request("0|CLOSE|S|reason|S|any-reason")
        with self.assertRaises(Exception):
            self.assert_reply()

    def test_subscribe(self):
        self.do_init_and_skip()
        self.do_subscription('item-nosnap')
        # As snapshot is not available for this item, an EOS notification
        # is also expected; the SDK library will send it even before
        # the SUB reply, which is allowed
        self.assert_notify("EOS|S|item-nosnap|S|10000010c3e4d0462")
        self.assert_reply("10000010c3e4d0462|SUB|V")
        item_name = self.adapter.subscribed.get()
        self.adapter.subscribed.task_done()
        self.assertEqual(item_name, "item-nosnap")

    def test_subscribe_to_more_items(self):
        self.do_init_and_skip()
        self.do_subscription_with_request_id("10000010c3e4d0462", 'item1-nosnap')
        # As snapshot is not available for this item, an EOS notification
        # is also expected; the SDK library will send it even before
        # the SUB reply, which is allowed
        self.assert_notify("EOS|S|item1-nosnap|S|10000010c3e4d0462")
        self.assert_reply("10000010c3e4d0462|SUB|V")

        self.do_subscription_with_request_id("20000010c3e4d0462", 'item2-nosnap')
        # As snapshot is not available for this item, an EOS notification
        # is also expected; the SDK library will send it even before
        # the SUB reply, which is allowed
        self.assert_notify("EOS|S|item2-nosnap|S|20000010c3e4d0462")
        self.assert_reply("20000010c3e4d0462|SUB|V")

        self.do_subscription_with_request_id("30000010c3e4d0462", 'item3-nosnap')
        # As snapshot is not available for this item, an EOS notification
        # is also expected; the SDK library will send it even before
        # the SUB reply, which is allowed
        self.assert_notify("EOS|S|item3-nosnap|S|30000010c3e4d0462")
        self.assert_reply("30000010c3e4d0462|SUB|V")

    def test_subscribe_with_subscribe_exception(self):
        self.do_init_and_skip()
        self.do_subscription('item-err-1')
        self.assert_reply("10000010c3e4d0462|SUB|EU|Subscription+Error")

    def test_subscribe_with_failure_exception(self):
        self.do_init_and_skip()
        self.do_subscription('item-err-2')
        self.assert_reply("10000010c3e4d0462|SUB|EF|Failure+Error")

    def test_subscribe_with_genieric_exception(self):
        self.do_init_and_skip()
        self.do_subscription('item-err-3')
        self.assert_reply("10000010c3e4d0462|SUB|E|Error")

    def test_malformed_subscribe(self):
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|SUB|S1|item")
        self.assert_notify("FAL|E|Unknown+type+%27S1%27+found+while+parsing+"
                           "SUB+request")
        self.send_request("10000010c3e4d0462|SUB|S||")
        self.assert_notify("FAL|E|Token+not+found+while+parsing+SUB+request")

    def test_unsubscribe(self):
        self.do_init_and_skip()
        self.do_subscription_and_skip('item')
        item_name = self.adapter.subscribed.get()
        self.adapter.subscribed.task_done()
        self.assertEqual(item_name, "item")
        self.do_unsubscription('item')
        self.assert_reply("10000010c3e4d0463|USB|V")

    def test_unsubscribe_with_unsubscribe_exception(self):
        self.do_init_and_skip()
        self.do_subscription_and_skip('item-err-4')
        self.do_unsubscription('item-err-4')
        self.assert_reply("10000010c3e4d0463|USB|EU|Subscription+Error")

    def test_unsubscribe_with_failure_exception(self):
        self.do_init_and_skip()
        self.do_subscription_and_skip('item-err-5')
        self.do_unsubscription('item-err-5')
        self.assert_reply("10000010c3e4d0463|USB|EF|Failure+Error")

    def test_unsubscribe_with_genieric_exception(self):
        self.do_init_and_skip()
        self.do_subscription_and_skip('item-err-6')
        self.do_unsubscription('item-err-6')
        self.assert_reply("10000010c3e4d0463|USB|E|Error")

    def test_unsubscribe_without_subscription(self):
        self.do_init_and_skip()
        self.do_unsubscription('item')
        with self.assertRaises(Exception):
            self.assert_reply(timeout=0.5)
        LOG.exception("Timeout expired")

    def test_malformed_unsubscribe(self):
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|USB|S1|item")
        self.assert_notify("FAL|E|Unknown+type+%27S1%27+found+while+parsing+"
                           "USB+request")
        self.send_request("10000010c3e4d0462|USB|S||")
        self.assert_notify("FAL|E|Token+not+found+while+parsing+USB+request")

    def test_eos(self):
        self.do_init_and_skip()
        self.do_subscription_and_skip('item')
        self.adapter.listener.end_of_snapshot("item")
        self.assert_notify("EOS|S|item|S|10000010c3e4d0462")

    def test_cls(self):
        self.do_init_and_skip()
        self.do_subscription_and_skip('item-nosnap')
        # As snapshot is not available for this item, an EOS notification
        # is also expected, but it is skipped as well

        self.adapter.listener.clear_snapshot("item-nosnap")
        self.assert_notify("CLS|S|item-nosnap|S|10000010c3e4d0462")

    def test_update_with_str_value(self):
        self.do_init_and_skip()
        self.do_subscription_and_skip("item-nosnap")
        # As snapshot is not available for this item, an EOS notification
        # is also expected, but it is skipped as well

        # Usage of OrderdDict with the only purpose of respecting the order
        # expressed in the assert statement.
        events_map = OrderedDict([("field1", "value1"),
                                  ("field2", "value2")])
        self.adapter.listener.update("item-nosnap", events_map, False)

        self.assert_notify("UD3|S|item-nosnap|S|10000010c3e4d0462|B|0|S|field1|S"
                           "|value1|S|field2|S|value2")

    def test_massive_update(self):
        self.do_init_and_skip()
        self.do_subscription_and_skip("item-nosnap")
        # As snapshot is not available for this item, an EOS notification
        # is also expected, but it is skipped as well

        # Usage of OrderdDict with the only purpose of respecting the order
        # expressed in the assert statement.
        for i in range(0, 1000):
            events_map = OrderedDict([("field1", "value1"),
                                      ("field2", str(i))])

            self.adapter.listener.update("item-nosnap", events_map, False)
            self.assert_notify("UD3|S|item-nosnap|S|10000010c3e4d0462|B|0|S|field1|S"
                               "|value1|S|field2|S|{}".format(i))

    def test_update_with_byte_value(self):
        self.do_init_and_skip()
        self.do_subscription_and_skip("item-nosnap")
        # As snapshot is not available for this item, an EOS notification
        # is also expected, but it is skipped as well

        # Usage of OrderdDict with the only purpose of respecting the order
        # expressed in the assert statement.
        events_map = OrderedDict([("pct_change", b'0.44'),
                                  ("last_price", b'6.82'),
                                  ("time", b'12:48:24')])
        self.adapter.listener.update('item-nosnap', events_map, True)

        self.assert_notify("UD3|S|item-nosnap|S|10000010c3e4d0462|B|1|S|pct_change|"
                           "Y|MC40NA==|S|last_price|Y|Ni44Mg==|S|time|Y|"
                           "MTI6NDg6MjQ=")

    def test_update_with_none_value(self):
        self.do_init_and_skip()
        self.do_subscription_and_skip("item-nosnap")
        # As snapshot is not available for this item, an EOS notification
        # is also expected, but it is skipped as well

        # Usage of OrderdDict with the only purpose of respecting the order
        # expressed in the assert statement.
        events_map = OrderedDict([("pct_change", b'0.44'),
                                  ("last_price", b'6.82'),
                                  ("time", None)])
        self.adapter.listener.update('item-nosnap', events_map, True)
        self.assert_notify("UD3|S|item-nosnap|S|10000010c3e4d0462|B|1|S|pct_change|"
                           "Y|MC40NA==|S|last_price|Y|Ni44Mg==|S|time|S|#")

    def test_update_with_empty_value(self):
        self.do_init_and_skip()
        self.do_subscription_and_skip("item-nosnap")
        # As snapshot is not available for this item, an EOS notification
        # is also expected, but it is skipped as well

        # Usage of OrderdDict with the only purpose of respecting the order
        # expressed in the assert statement.
        events_map = OrderedDict([("pct_change", b'0.44'),
                                  ("last_price", b'6.82'),
                                  ("time", "")])
        self.adapter.listener.update('item-nosnap', events_map, True)
        self.assert_notify("UD3|S|item-nosnap|S|10000010c3e4d0462|B|1|S|pct_change|"
                           "Y|MC40NA==|S|last_price|Y|Ni44Mg==|S|time|S|$")

    def test_failure(self):
        self.do_init_and_skip()
        self.adapter.listener.failure(Exception("Generic exception"))
        self.assert_notify("FAL|E|Generic+exception")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'DataProviderTest.testName']
    unittest.main()
