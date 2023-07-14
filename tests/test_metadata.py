import unittest
import time
from multiprocessing import cpu_count
from lightstreamer_adapter.server import MetadataProviderServer
from lightstreamer_adapter.interfaces.metadata import (MetadataProvider,
                                                       MpnDeviceInfo,
                                                       Mode,
                                                       AccessError,
                                                       CreditsError,
                                                       NotificationError,
                                                       ConflictingSessionError,
                                                       SchemaError,
                                                       ItemsError,
                                                       MetadataProviderError,
                                                       MpnPlatformType,
                                                       TableInfo,
                                                       MpnSubscriptionInfo)
from .common import (RemoteAdapterBase, KeepaliveConstants)

# Specify here the number of your CPU cores
EXPECTED_CPU_CORES = cpu_count()

# Keep aligned with the ARI version currently implemented
basic_version = '1.8.3'


def assert_credentials_response(remote_adapter):
    remote_adapter.assert_reply("1|RAC|S|enableClosePacket|S|true"
                                "|S|SDK|S|Python+Adapter+SDK")

class MetadataProviderTestClass(MetadataProvider):

    def __init__(self, collector):
        self.collector = collector
        self.config_file = None

    def initialize(self, parameters, config_file=None):
        self.config_file = config_file
        if parameters:
            # "parX" are provided only when testing initialisation of
            # local parameter through "set_adaper_params" method.
            if "par1" not in parameters:
                if "adapters_conf.id" not in parameters:
                    raise MetadataProviderError("The ID must be supplied")
                if "proxy.instance_id" not in parameters:
                    raise RuntimeError("Exception")
            self.collector['params'] = parameters

    def get_items(self, user, session_id, group):
        self.collector.update({'user': user,
                               'sessionId': session_id,
                               'group': group})

        if user is None:
            raise RuntimeError("NULL User")

        if not user:
            raise ItemsError("Empty User")

        if group == "no items":
            return None

        if group == "get invalid":
            # Returning non string list to enforce raising of an exception
            # while encoding the reply.
            return [1, False, 2.3]

        return super(MetadataProviderTestClass, self).get_items(user,
                                                                session_id,
                                                                group)

    def get_allowed_max_bandwidth(self, user):
        if user == "get invalid":
            return "Wrong Allowed Max Bandwidth Type"
        return 12.3

    def wants_tables_notification(self, user):
        return True

    def get_distinct_snapshot_length(self, item):
        if item == "item_1":
            return 10

        if item == "item_2":
            return 20

        if item == "item_4":
            raise RuntimeError("Error for provided item")

        if item == "get invalid":
            return "Wrong Distinct Snapshot Length Type"

        return 0

    def get_min_source_frequency(self, item):
        if item == "item_1":
            return 4.5

        if item == "item_2":
            return 7.3

        return 0.0

    def mode_may_be_allowed(self, item, mode):
        if item == "item_1":
            if mode in [Mode.MERGE, Mode.DISTINCT]:
                return True

        if item == "item_2":
            if mode in [Mode.RAW, Mode.MERGE, Mode.COMMAND]:
                return True

        return False

    def get_allowed_max_item_frequency(self, user, item):
        if item == "item_1":
            return 170.5

        if item == "item_2":
            return 27.3

        return 0.0

    def get_allowed_buffer_size(self, user, item):
        if item == "item_1":
            return 30

        if item == "item_2":
            return 40

        if item == "get invalid":
            return "Wrong Allowed Buffer Size"

        return 0

    def ismode_allowed(self, user, item, mode):
        if item == "item_1":
            return True

        if item == "item_2" and mode in [Mode.RAW, Mode.MERGE]:
            return True

        if user == "user2":
            raise RuntimeError("No user allowed")

        return False

    def notify_user(self, user, password, http_headers):
        self.collector.update({'user': user,
                               'remote_password': password,
                               'httpHeaders': http_headers})

        if user == "user1":
            raise CreditsError(10, "CreditsError", "User not allowed")

        if user == "user2":
            raise RuntimeError("Error while notifying")

        if user is None:
            raise AccessError("A NULL user is not allowed")

        if not user.strip():
            raise AccessError("An empty user is not allowed")

    def notify_user_with_principal(self, user, password, http_headers,
                                   client_principal=None):
        self.collector.update({'user': user,
                               'remote_password': password,
                               'httpHeaders': http_headers,
                               'clientPrincipal': client_principal})

        self.notify_user(user, password, http_headers)

    def notify_new_session(self, user, session_id, client_context):
        if user == "user1":
            raise CreditsError(9, "CreditsError", "User not allowed")

        if user == "user2":
            raise NotificationError("NotificationError")

        if user == "user3":
            raise ConflictingSessionError(11, "ConflictingSessionError",
                                          "conflictingSessionID",
                                          "clientErrorMsg")

        if user == "user4":
            raise RuntimeError("Error while notifying")

        self.collector.update({'user': user,
                               'sessionId': session_id,
                               'client_context': client_context})

    def notify_session_close(self, session_id):
        if session_id is None:
            raise RuntimeError("NULL SessionId")

        # Empty session_id
        if not session_id:
            raise NotificationError("Empty SessionId")

        self.collector['sessionId'] = session_id

    def get_schema(self, user, session_id, group, schema):
        self.collector.update({'user': user,
                               'sessionId': session_id,
                               'group': group,
                               'schema': schema})

        if user is None:
            raise ItemsError("NULL User")

        if not user:
            raise RuntimeError("Empty User")

        if schema == "shortA":
            raise SchemaError("SchemaError")

        if schema == "no schema":
            return None

        if schema == "get invalid":
            # Returning non string list to enforce raising of an exception
            # while encoding the reply.
            return [1, False, 2.3]

        return super(MetadataProviderTestClass, self).get_schema(user,
                                                                 session_id,
                                                                 group,
                                                                 schema)

    def notify_user_message(self, user, session_id, message):
        if user == "user2":
            raise NotificationError("NotificationError")

        if user == "user3":
            raise CreditsError(4, "CreditsError", "clientErrorMsg")

        if user == "user4":
            raise RuntimeError("Exception")

        self.collector.update({'user': user,
                               'sessionId': session_id,
                               'message': message})

    def notify_new_tables(self, user, session_id, tables):
        if user == "user1":
            raise CreditsError(10, "CreditsError", "clientErrorMsg")

        if user == "user2":
            raise NotificationError("NotificationError")

        if user == "user3":
            raise RuntimeError("Exception")

        self.collector.update({'user': user,
                               'sessionId': session_id,
                               'tableInfos': tables})

    def notify_tables_close(self, session_id, tables):
        if session_id == "S8f3da29cfc463220T5454539":
            raise NotificationError("NotificationError")

        if session_id == "S8f3da29cfc463220T5454540":
            raise RuntimeError("Exception")

        self.collector.update({'sessionId': session_id,
                               'tableInfos': tables})

    def notify_mpn_device_access(self, user, session_id, device):
        if user is None:
            raise NotificationError("NotificationError")

        if not user:
            raise CreditsError(10, "CreditsError", "clientErrorMsg")

        if user == "user2":
            raise RuntimeError("Exception")

        self.collector.update({"user": user,
                               "sessionId": session_id,
                               "mpnDeviceInfo": device})

    def notify_mpn_subscription_activation(self, user, session_id, table,
                                           mpn_subscription):
        if user is None:
            raise NotificationError("NotificationError")

        if not user:
            raise CreditsError(10, "CreditsError", "clientErrorMsg")

        if user == "user2":
            raise RuntimeError("Exception")

        self.collector.update({"user": user,
                               "sessionId": session_id,
                               "table": table,
                               "mpn_subscription": mpn_subscription})

    def notify_mpn_device_token_change(self, user, session_id, device,
                                       new_device_token):
        if user is None:
            raise NotificationError("NotificationError")

        if not user:
            raise CreditsError(10, "CreditsError", "clientErrorMsg")

        if user == "user2":
            raise RuntimeError("Exception")
        self.collector.update({"user": user,
                               "sessionId": session_id,
                               "mpnDeviceInfo": device,
                               "newDeviceToken": new_device_token})


class TableInfoTest(unittest.TestCase):

    def test_properties(self):
        tb1 = TableInfo(win_index=1,
                        mode=Mode.MERGE,
                        group="nasdaq100_AA_AL",
                        schema="short",
                        first_idx=1,
                        last_idx=5)

        self.assertEqual(1, tb1.win_index)
        self.assertEqual(Mode.MERGE, tb1.mode)
        self.assertEqual("nasdaq100_AA_AL", tb1.group)
        self.assertEqual("short", tb1.schema)
        self.assertEqual(1, tb1.min)
        self.assertEqual(5, tb1.max)

    def test_equality(self):
        tb1 = TableInfo(win_index=1,
                        mode=Mode.MERGE,
                        group="nasdaq100_AA_AL",
                        schema="short",
                        first_idx=1,
                        last_idx=5)

        tb2 = TableInfo(win_index=1,
                        mode=Mode.MERGE,
                        group="nasdaq100_AA_AL",
                        schema="short",
                        first_idx=1,
                        last_idx=5)

        self.assertEqual(tb1, tb2)

    def test_not_equality(self):
        tb1 = TableInfo(win_index=1,
                        mode=Mode.DISTINCT,
                        group="nasdaq100_AA_AL",
                        schema="short",
                        first_idx=1,
                        last_idx=5)

        tb2 = TableInfo(win_index=1,
                        mode=Mode.MERGE,
                        group="nasdaq100_AA_AL",
                        schema="short",
                        first_idx=1,
                        last_idx=5)

        self.assertNotEqual(tb1, tb2)


class MpnDeviceInfoTest(unittest.TestCase):

    def test_properties(self):
        device_info_1 = MpnDeviceInfo(MpnPlatformType.APPLE, "applicationId",
                                      "deviceToken")

        self.assertEqual(MpnPlatformType.APPLE,
                         device_info_1.mpn_platform_type)
        self.assertEqual("applicationId", device_info_1.application_id)
        self.assertEqual("deviceToken", device_info_1.device_token)

    def test_equality(self):
        device_info_1 = MpnDeviceInfo(MpnPlatformType.APPLE, "applicationId",
                                      "deviceToken")
        device_info_2 = MpnDeviceInfo(MpnPlatformType.APPLE, "applicationId",
                                      "deviceToken")
        self.assertEqual(device_info_1, device_info_2)

    def test_not_equality(self):
        device_info_1 = MpnDeviceInfo(MpnPlatformType.APPLE, "applicationId",
                                      "deviceToken")
        device_info_2 = MpnDeviceInfo(MpnPlatformType.GOOGLE, "applicationId",
                                      "deviceToken")
        self.assertNotEqual(device_info_1, device_info_2)

        device_info_2 = MpnDeviceInfo(MpnPlatformType.APPLE, "applicationId2",
                                      "deviceToken")
        self.assertNotEqual(device_info_1, device_info_2)

        device_info_2 = MpnDeviceInfo(MpnPlatformType.APPLE, "applicationId",
                                      "deviceToken2")
        self.assertNotEqual(device_info_1, device_info_2)


class MpnSubscriptionTest(unittest.TestCase):

    def test_properties(self):
        device = MpnDeviceInfo(MpnPlatformType.APPLE, "applicationId",
                               "deviceToken")
        subscription = MpnSubscriptionInfo(
            device=device,
            notification_format="{\"aps\":{\"alert\":\"${message}\","
                                "\"badge\":\"AUTO\"},\"acme2\":"
                                "[\"${tag1}\",\"${tag2}\"]}",
            trigger="Double.parseDouble(${last_price}) > 1000.0")

        self.assertEqual(device, subscription.device)
        self.assertEqual("Double.parseDouble(${last_price}) > 1000.0",
                         subscription.trigger)
        self.assertEqual(("{\"aps\":{\"alert\":\"${message}\","
                          "\"badge\":\"AUTO\"},\"acme2\":"
                          "[\"${tag1}\",\"${tag2}\"]}"),
                         subscription.notification_format)


class MetadataProviderTest(unittest.TestCase):

    def setUp(self):
        self.provider = MetadataProvider()

    def test_allowed_buffer_size(self):
        buffer_size = self.provider.get_allowed_buffer_size("", "")
        self.assertIsInstance(buffer_size, int)
        self.assertEqual(0, buffer_size)

    def test_allowed_max_bandwidth(self):
        max_bandwidth = self.provider.get_allowed_max_bandwidth("user")
        self.assertIsInstance(max_bandwidth, float)
        self.assertEqual(0, max_bandwidth)

    def test_allowed_max_item_frequency(self):
        max_frequency = self.provider.get_allowed_max_item_frequency("user",
                                                                     "item1")
        self.assertIsInstance(max_frequency, float)
        self.assertEqual(0, max_frequency)

    def test_distinct_snapshot_length(self):
        snapshot_length = self.provider.get_distinct_snapshot_length("item1")
        self.assertIsInstance(snapshot_length, int)
        self.assertEqual(0, snapshot_length)

    def test_min_source_frequency(self):
        min_frequency = self.provider.get_min_source_frequency("item1")
        self.assertIsInstance(min_frequency, float)
        self.assertEqual(0, min_frequency)

    def test_items(self):
        items = self.provider.get_items("", "", "item1 item2")
        self.assertEqual(["item1", "item2"], items)

        items = self.provider.get_items("", "", "item1")
        self.assertEqual(["item1"], items)

        items = self.provider.get_items("", "", "")
        self.assertEqual([''], items)

    def test_schema(self):
        fields = self.provider.get_schema("", "", "item1", "field1 field2")
        self.assertEqual(["field1", "field2"], fields)

        fields = self.provider.get_schema("", "", "item1", "field1")
        self.assertEqual(["field1"], fields)

        fields = self.provider.get_schema("", "", "item1", "")
        self.assertEqual([""], fields)

    def test_ismode_allowed(self):
        ismode_allowed = self.provider.ismode_allowed("", "", None)
        self.assertIsInstance(ismode_allowed, bool)
        self.assertTrue(ismode_allowed)

    def test_mode_mabye_allowed(self):
        mode_maybe_allowed = self.provider.mode_may_be_allowed("", None)
        self.assertIsInstance(mode_maybe_allowed, bool)
        self.assertTrue(mode_maybe_allowed)

    def test_wants_table_notification(self):
        mode_maybe_allowed = self.provider.wants_tables_notification("")
        self.assertIsInstance(mode_maybe_allowed, bool)
        self.assertFalse(mode_maybe_allowed)


class MetadataProviderServerConstructionTest(unittest.TestCase):

    def test_start_with_error(self):
        remote_server = MetadataProviderServer(
            MetadataProviderTestClass({}),
            RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS)
        with self.assertRaises(Exception) as err:
            remote_server.start()

        the_exception = err.exception
        self.assertIsInstance(the_exception, MetadataProviderError)
        self.assertEqual(str(the_exception),
                         "Caught an error during the initialization phase")

    def test_not_right_adapter(self):
        with self.assertRaises(TypeError) as type_error:
            MetadataProviderServer(
                {},
                RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS)

        the_exception = type_error.exception
        self.assertIsInstance(the_exception, TypeError)
        self.assertEqual(str(the_exception),
                         "The provided adapter is not a subclass of "
                         "lightstreamer_adapter.interfaces.MetadataProvider")

    def test_default_properties(self):
        # Test default properties
        server = MetadataProviderServer(
            MetadataProviderTestClass({}),
            RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS)
        self.assertEqual('#', server.name[0])
        self.assertEqual(KeepaliveConstants.DEFAULT.value, server.keep_alive)
        self.assertEqual(EXPECTED_CPU_CORES, server.thread_pool_size)
        self.assertIsNone(server.remote_user)
        self.assertIsNone(server.remote_password)

    def test_thread_pool_size(self):
        # Test non default properties
        server = MetadataProviderServer(
            MetadataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS,
            thread_pool_size=2)
        self.assertEqual(2, server.thread_pool_size)

        server = MetadataProviderServer(
            MetadataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS,
            thread_pool_size=0)
        self.assertEqual(EXPECTED_CPU_CORES, server.thread_pool_size)

        server = MetadataProviderServer(
            MetadataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS,
            thread_pool_size=-2)
        self.assertEqual(EXPECTED_CPU_CORES, server.thread_pool_size)

        server = MetadataProviderServer(
            MetadataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS,
            thread_pool_size=None)
        self.assertEqual(EXPECTED_CPU_CORES, server.thread_pool_size)

    def test_keep_alive_value(self):
        # Test non default properties
        server = MetadataProviderServer(
            MetadataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS,
            keep_alive=2)
        self.assertEqual(2, server.keep_alive)

        server = MetadataProviderServer(
            MetadataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS,
            keep_alive=0)
        self.assertEqual(0, server.keep_alive)

        server = MetadataProviderServer(
            MetadataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS,
            keep_alive=-2)
        self.assertEqual(0, server.keep_alive)

        server = MetadataProviderServer(
            MetadataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS,
            keep_alive=None)
        self.assertEqual(KeepaliveConstants.DEFAULT.value, server.keep_alive)

    def test_unset_remote_credentials(self):
        server = MetadataProviderServer(
            MetadataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS)
        self.assertIsNone(server.remote_user)
        self.assertIsNone(server.remote_password)

    def test_remote_credentialis(self):
        server = MetadataProviderServer(
            MetadataProviderTestClass({}),
            address=RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS)
        server.remote_user = "user"
        server.remote_password = "password"
        self.assertEqual("user", server.remote_user)
        self.assertEqual("password", server.remote_password)


class MetadataProviderServerInitializationTest(RemoteAdapterBase):

    def __init__(self, method_name):
        super(MetadataProviderServerInitializationTest, self).__init__(
            method_name)
        self.collector = {}
        self.adapter = MetadataProviderTestClass(self.collector)

    def setup_remote_adapter(self, keep_alive=None, config=None, params=None,
                             username=None, password=None):
        remote_server = MetadataProviderServer(
            adapter=self.adapter,
            address=RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS,
            keep_alive=keep_alive)
        remote_server.adapter_config = config
        remote_server.adapter_params = params
        remote_server.remote_user = username
        remote_server.remote_password = password
        self.launch_remote_server(remote_server, set_exception_handler=True)

    def do_init_and_skip(self):
        # RAC reply always received.
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.skip_messages()

    def test_no_kalive_hint_and_no_configured_kalive(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO"
                          "|S|proxy.instance_id|S|hewbc3ikbbctyui"
                          "|S|ARI.version|S|" + basic_version)
        self.assert_reply("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertEqual({"adapters_conf.id": "DEMO", "proxy.instance_id":
                          "hewbc3ikbbctyui"}, self.collector['params'])
        self.assertIsNone(self.adapter.config_file)

        self.assertEqual(KeepaliveConstants.STRICTER.value,
                         self.remote_server.keep_alive)

    def test_no_kalive_hint_and_configured_kalive(self):
        configured_keepalive = 5
        self.setup_remote_adapter(configured_keepalive)
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO"
                          "|S|proxy.instance_id|S|hewbc3ikbbctyui"
                          "|S|ARI.version|S|" + basic_version)
        self.assert_reply("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertEqual({"adapters_conf.id": "DEMO", "proxy.instance_id":
                          "hewbc3ikbbctyui"}, self.collector['params'])
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(configured_keepalive, self.remote_server.keep_alive)

    def test_negative_kalive_hint_and_no_configured_kalive(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO"
                          "|S|keepalive_hint.millis|S|-510"
                          "|S|proxy.instance_id|S|hewbc3ikbbctyui"
                          "|S|ARI.version|S|" + basic_version)
        self.assert_reply("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertEqual({"adapters_conf.id": "DEMO", "proxy.instance_id":
                          "hewbc3ikbbctyui"}, self.collector['params'])
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(KeepaliveConstants.DEFAULT.value,
                         self.remote_server.keep_alive)

    def test_negative_kalive_hint_and_configured_kalive(self):
        configured_keepalive = 6
        self.setup_remote_adapter(configured_keepalive)
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO"
                          "|S|keepalive_hint.millis|S|-500"
                          "|S|proxy.instance_id|S|hewbc3ikbbctyui"
                          "|S|ARI.version|S|" + basic_version)
        self.assert_reply("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertEqual({"adapters_conf.id": "DEMO", "proxy.instance_id":
                          "hewbc3ikbbctyui"}, self.collector['params'])
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(configured_keepalive, self.remote_server.keep_alive)

    def test_kalive_hint_lt_default_and_no_configured_kalive(self):
        expected_keepalive = 9
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO|S|"
                          "keepalive_hint.millis|S|9000"
                          "|S|proxy.instance_id|S|hewbc3ikbbctyui"
                          "|S|ARI.version|S|" + basic_version)
        self.assert_reply("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertEqual({"adapters_conf.id": "DEMO", "proxy.instance_id":
                          "hewbc3ikbbctyui"}, self.collector['params'])
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(expected_keepalive, self.remote_server.keep_alive)

    def test_kalive_hint_lt_default_and_min_and_no_configured_kalive(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO|S|"
                          "keepalive_hint.millis|S|500"
                          "|S|proxy.instance_id|S|hewbc3ikbbctyui"
                          "|S|ARI.version|S|" + basic_version)
        self.assert_reply("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertEqual({"adapters_conf.id": "DEMO", "proxy.instance_id":
                          "hewbc3ikbbctyui"}, self.collector['params'])
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(KeepaliveConstants.MIN.value,
                         self.remote_server.keep_alive)

    def test_kalive_hint_gt_default_and_no_configured_kalive(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO"
                          "|S|keepalive_hint.millis|S|11000"
                          "|S|proxy.instance_id|S|hewbc3ikbbctyui"
                          "|S|ARI.version|S|" + basic_version)
        self.assert_reply("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertEqual({"adapters_conf.id": "DEMO", "proxy.instance_id":
                          "hewbc3ikbbctyui"}, self.collector['params'])
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(KeepaliveConstants.DEFAULT.value,
                         self.remote_server.keep_alive)

    def test_kalive_lt_configured_kalive(self):
        expected_keepalive = 4
        configured_keepalive = 5
        self.setup_remote_adapter(configured_keepalive)
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO|S|"
                          "keepalive_hint.millis|S|4000"
                          "|S|proxy.instance_id|S|hewbc3ikbbctyui"
                          "|S|ARI.version|S|" + basic_version)
        self.assert_reply("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertEqual({"adapters_conf.id": "DEMO", "proxy.instance_id":
                          "hewbc3ikbbctyui"}, self.collector['params'])
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(expected_keepalive, self.remote_server.keep_alive)

    def test_kalive_less_then_configured_kalive_and_min(self):
        configured_keepalive = 5
        self.setup_remote_adapter(configured_keepalive)
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO"
                          "|S|keepalive_hint.millis|S|500"
                          "|S|proxy.instance_id|S|hewbc3ikbbctyui"
                          "|S|ARI.version|S|" + basic_version)
        self.assert_reply("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertEqual({"adapters_conf.id": "DEMO", "proxy.instance_id":
                          "hewbc3ikbbctyui"}, self.collector['params'])
        self.assertIsNone(self.adapter.config_file)
        self.assertEqual(KeepaliveConstants.MIN.value,
                         self.remote_server.keep_alive)

    def test_default_keep_alive(self):
        self.setup_remote_adapter(2)
        assert_credentials_response(self)
        # Receive a KEEPALIVE message because no request has been issued
        for _ in range(0, 1):
            start = time.time()
            self.assert_reply("KEEPALIVE", timeout=2.1, skip_keepalive=False)
            end = time.time()
            self.assertGreaterEqual(end - start, 1.99)

    def test_init(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO|S|"
                          "proxy.instance_id|S|hewbc3ikbbctyui"
                          "|S|ARI.version|S|" + basic_version)

        self.assert_reply("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertEqual({"adapters_conf.id": "DEMO", "proxy.instance_id":
                          "hewbc3ikbbctyui"}, self.collector['params'])
        self.assertIsNone(self.adapter.config_file)

    def test_init_with_adapter_config(self):
        self.setup_remote_adapter(config="config.file")
        self.do_init_and_skip()
        self.assertEqual("config.file", self.adapter.config_file)

    def test_init_with_local_params(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.remote_server.adapter_params = {"par1": "val1", "par2": "val2"}
        self.send_request("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)

        self.assert_reply("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertDictEqual({"par1": "val1",
                              "par2": "val2"},
                             self.collector['params'])

    def test_init_with_remote_params(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO|S|"
                          "proxy.instance_id|S|hewbc3ikbbctyui"
                          "|S|ARI.version|S|" + basic_version)

        self.assert_reply("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertDictEqual({"adapters_conf.id": "DEMO",
                              "proxy.instance_id": "hewbc3ikbbctyui"},
                             self.collector['params'])

    def test_init_with_local_and_remote_params(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.remote_server.adapter_params = {"proxy.instance_id":
                                             "my_local_meta_provider"}
        self.send_request("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO|S|"
                          "proxy.instance_id|S|hewbc3ikbbctyui"
                          "|S|ARI.version|S|" + basic_version)
        self.assert_reply("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assertDictEqual({"adapters_conf.id": "DEMO",
                              "proxy.instance_id":
                              "my_local_meta_provider"},
                             self.collector['params'])

    def test_init_with_protocol_1_8_0(self):
        self.setup_remote_adapter(params={"proxy.instance_id":
                                          "my_local_meta_provider"})
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|"
                          "adapters_conf.id|S|DEMO|S|proxy.instance_id|S|"
                          "hewbc3ikbbctyui")
        self.assert_reply("10000010c3e4d0462|MPI|V")
        self.assertDictEqual({"adapters_conf.id": "DEMO",
                              "proxy.instance_id":
                              "my_local_meta_provider"},
                             self.collector['params'])

    def test_init_with_protocol_1_8_1(self):
        self.setup_remote_adapter(params={"proxy.instance_id":
                                          "my_local_meta_provider"})
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|ARI.version|S|1.8.1|S|"
                          "adapters_conf.id|S|DEMO|S|proxy.instance_id|S|"
                          "hewbc3ikbbctyui")
        self.assert_reply("10000010c3e4d0462|MPI|E|Unsupported+reserved+"
                          "protocol+version+number%3A+1.8.1")
        self.assertFalse('params' in self.collector)

    def test_init_with_protocol_1_8_2(self):
        self.setup_remote_adapter(params={"proxy.instance_id":
                                          "my_local_meta_provider"})
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|ARI.version|S|1.8.2|S|"
                          "adapters_conf.id|S|DEMO|S|proxy.instance_id|S|"
                          "hewbc3ikbbctyui")
        self.assert_reply("10000010c3e4d0462|MPI|S|ARI.version|S|1.8.2")
        self.assertDictEqual({"adapters_conf.id": "DEMO",
                              "proxy.instance_id":
                              "my_local_meta_provider"},
                             self.collector['params'])

    def test_init_with_protocol_1_8_3(self):
        self.setup_remote_adapter(params={"proxy.instance_id":
                                          "my_local_meta_provider"})
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|ARI.version|S|1.8.3|S|"
                          "adapters_conf.id|S|DEMO|S|proxy.instance_id|S|"
                          "hewbc3ikbbctyui")
        self.assert_reply("10000010c3e4d0462|MPI|S|ARI.version|S|1.8.3")
        self.assertDictEqual({"adapters_conf.id": "DEMO",
                              "proxy.instance_id":
                              "my_local_meta_provider"},
                             self.collector['params'])

    def test_init_with_protocol_1_8_4(self):
        self.setup_remote_adapter(params={"proxy.instance_id":
                                          "my_local_meta_provider"})
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|ARI.version|S|1.8.4|S|"
                          "adapters_conf.id|S|DEMO|S|proxy.instance_id|S|"
                          "hewbc3ikbbctyui")
        self.assert_reply("10000010c3e4d0462|MPI|S|ARI.version|S|1.8.3")
        self.assertDictEqual({"adapters_conf.id": "DEMO",
                              "proxy.instance_id":
                              "my_local_meta_provider"},
                             self.collector['params'])

    def test_init_with_protocol_1_9_0(self):
        self.setup_remote_adapter(params={"proxy.instance_id":
                                          "my_local_meta_provider"})
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|ARI.version|S|1.9.0|S|"
                          "adapters_conf.id|S|DEMO|S|proxy.instance_id|S|"
                          "hewbc3ikbbctyui")
        self.assert_reply("10000010c3e4d0462|MPI|S|ARI.version|S|1.8.3")
        self.assertDictEqual({"adapters_conf.id": "DEMO",
                              "proxy.instance_id":
                              "my_local_meta_provider"},
                             self.collector['params'])

    def test_init_with_protocol_1_9_1(self):
        self.setup_remote_adapter(params={"proxy.instance_id":
                                          "my_local_meta_provider"})
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|ARI.version|S|1.9.1|S|"
                          "adapters_conf.id|S|DEMO|S|proxy.instance_id|S|"
                          "hewbc3ikbbctyui")
        self.assert_reply("10000010c3e4d0462|MPI|S|ARI.version|S|1.8.3")
        self.assertDictEqual({"adapters_conf.id": "DEMO",
                              "proxy.instance_id":
                              "my_local_meta_provider"},
                             self.collector['params'])

    def test_init_with_metadata_provider_exception(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|proxy.instance_id|S|"
                          "hewbc3ikbbctyui"
                          "|S|ARI.version|S|" + basic_version)
        self.assert_reply("10000010c3e4d0462|MPI|EM|The+ID+must+be+supplied")

    def test_init_with_generic_exception(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO")
        self.assert_reply("10000010c3e4d0462|MPI|E|Exception")

    def test_malformed_init_for_unkown_token_type(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request('10000010c3e4d0462|MPI|S|adapters_conf.id|S1|DEMO')
        self.assert_caught_exception("Unknown type 'S1' found while parsing "
                                     "MPI request")

    def test_malformed_init_for_invalid_number_of_tokens(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request('10000010c3e4d0462|MPI|S|')
        self.assert_caught_exception("Invalid number of tokens while parsing "
                                     "MPI request")

    def test_malformed_init_for_invalid_number_of_tokens2(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request('10000010c3e4d0462|MPI|S||')
        self.assert_caught_exception("Invalid number of tokens while parsing "
                                     "MPI request")

    def test_malformed_init_for_invalid_number_of_tokens3(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request('10000010c3e4d0462|MPI|S|  |')
        self.assert_caught_exception("Invalid number of tokens while parsing "
                                     "MPI request")

    def test_malformed_init_for_invalid_number_of_tokens4(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request('10000010c3e4d0462|MPI|S|id|S')
        self.assert_caught_exception("Invalid number of tokens while parsing "
                                     "MPI request")

    def test_init_init(self):
        self.setup_remote_adapter()
        # Test error when more than one MPI request is issued
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|MPI")
        self.assert_caught_exception("Unexpected late MPI request")

    def test_init_miss(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        # Test error when the very first request is not a MPI request
        self.send_request("10000010c3e4d0462|NUS|S|userX|S|remote_password|S|"
                          "host|S|www.mycompany.com")

        self.assert_caught_exception("Unexpected request NUS while waiting for"
                                     " MPI request")

    def test_close(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.assert_reply("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)

        self.send_request("0|CLOSE|S|reason|S|any-reason")
        self.assert_no_caught_exception()
        with self.assertRaises(Exception):
            self.assert_reply()

    def test_close_not_recognized_because_of_protocol_1_8_0(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI")
        self.assert_reply('10000010c3e4d0462|MPI|V')

        self.send_request("0|CLOSE|S|reason|S|any-reason")
        self.assert_no_caught_exception()
        with self.assertRaises(Exception):
            self.assert_reply()

    def test_close_not_recognized_because_of_protocol_1_8_2(self):
        self.setup_remote_adapter()
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI|S|ARI.version|S|1.8.2")
        self.assert_reply('10000010c3e4d0462|MPI|S|ARI.version|S|1.8.2')

        self.send_request("0|CLOSE|S|reason|S|any-reason")
        self.assert_no_caught_exception()
        with self.assertRaises(Exception):
            self.assert_reply()

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
        assert_credentials_response(self)


class MetadataProviderServerTest(RemoteAdapterBase):

    def on_setup(self):
        self.collector = {}
        # Configuring and starting MetadataProviderServer.
        adapter = MetadataProviderTestClass(self.collector)
        remote_server = MetadataProviderServer(
            adapter,
            RemoteAdapterBase.PROXY_METADATA_ADAPTER_ADDRESS)
        self.launch_remote_server(remote_server, set_exception_handler=True)

    def do_init_and_skip(self):
        # RAC reply always received.
        assert_credentials_response(self)
        self.send_request("10000010c3e4d0462|MPI"
                          "|S|ARI.version|S|" + basic_version)
        self.skip_messages()

    def test_notify_user(self):
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|NUS|S|userX|S|remote_password|S|"
                          "host|S|www.mycompany.com")

        self.assert_reply("10000010c3e4d0462|NUS|D|12.3|B|1")
        self.assertDictEqual({"user": "userX",
                              "remote_password": "remote_password",
                              "httpHeaders": {"host": "www.mycompany.com"}},
                             self.collector)

    def test_notify_user_with_no_http_headers(self):
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|NUS|S|userX|S|remote_password")
        self.assert_reply("10000010c3e4d0462|NUS|D|12.3|B|1")
        self.assertDictEqual({"user": "userX",
                              "remote_password": "remote_password",
                              "httpHeaders": {}},
                             self.collector)

    def test_notify_user_with_credits_exception(self):
        # Testing CreditsError
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|NUS|S|user1|S|remote_password|S|"
                          "host|S|www.mycompany.com")
        self.assert_reply("10000010c3e4d0462|NUS|EC|CreditsError|10|User+not+"
                          "allowed")

    def test_notify_user_with_access_exception(self):
        # Testing AccessError
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|NUS|S|$|S|remote_password|S|host|"
                          "S|www.mycompany.com")
        self.assert_reply("10000010c3e4d0462|NUS|EA|An+empty+user+is+not+"
                          "allowed")

    def test_notify_user_with_access_exception2(self):
        # Testing AccessError with null value for user
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|NUS|S|#|S|#|S|host|S|"
                          "www.mycompany.com|")
        self.assert_reply("10000010c3e4d0462|NUS|EA|A+NULL+user+is+not+"
                          "allowed")

    def test_notify_user_with_generic_exception(self):
        # Testing other errors
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|NUS|S|user2|S|remote_password|S|"
                          "host|S|www.mycompany.com")
        self.assert_reply("10000010c3e4d0462|NUS|E|Error+while+notifying")

    def test_invalid_notify_user(self):
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|NUS|S|get+invalid|S|"
                          "remote_password")
        self.assert_caught_exception("Not a float value: 'Wrong Allowed Max "
                                     "Bandwidth Type'")

    def test_malformed_notify_user(self):
        self.do_init_and_skip()
        # user and remote_password are missing.
        self.send_request("10000010c3e4d0462|NUS")
        self.assert_caught_exception("Token not found while parsing NUS "
                                     "request")

        # user and remote_password are missing.
        self.send_request("10000010c3e4d0462|NUS|S|")
        self.assert_caught_exception("Token not found while parsing NUS "
                                     "request")

        # Wrong token type for user.
        self.send_request("10000010c3e4d0462|NUS|S1|user")
        self.assert_caught_exception("Unknown type 'S1' found while parsing "
                                     "NUS request")

        # remote_password is missing
        self.send_request("10000010c3e4d0462|NUS|S|user")
        self.assert_caught_exception("Token not found while parsing NUS "
                                     "request")

        # remote_password is missing
        self.send_request("10000010c3e4d0462|NUS|S|user|S")
        self.assert_caught_exception("Token not found while parsing NUS "
                                     "request")

        # Wrong token type for remote_password.
        self.send_request("10000010c3e4d0462|NUS|S|user|S2")
        self.assert_caught_exception("Unknown type 'S2' found while parsing "
                                     "NUS request")

    def test_notify_user_auth(self):
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|NUA|S|userXA|S|remote_password|S|"
                          "<CLIENT_PRINCIPAL>|S|<HEADER_1>|S|<HEADER_VALUE1>|"
                          "S|<HEADER_2>|S|<HEADER_VALUE2>")

        self.assert_reply("10000010c3e4d0462|NUA|D|12.3|B|1")
        expected_dict = {"user": "userXA",
                         "remote_password": "remote_password",
                         "clientPrincipal": '<CLIENT_PRINCIPAL>',
                         "httpHeaders": {"<HEADER_1>": "<HEADER_VALUE1>",
                                         "<HEADER_2>": "<HEADER_VALUE2>"}}
        self.assertDictEqual(expected_dict, self.collector)

    def test_notify_user_with_auth_credits_exception(self):
        # Testing CreditsError
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|NUA|S|user1|S|remote_password|S|"
                          "<CLIENT_PRINCIPAL>|S|<HEADER_1>|S|<HEADER_VALUE1>|"
                          "S|<HEADER_2>|S|<HEADER_VALUE2>")
        self.assert_reply("10000010c3e4d0462|NUA|EC|CreditsError|10|User+not+"
                          "allowed")

    def test_notify_user_with_auth_access_exception(self):
        # Testing AccessError
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|NUA|S|$|S|remote_password|S|"
                          "<CLIENT_PRINCIPAL>|S|<HEADER_1>|S|<HEADER_VALUE1>|"
                          "S|<HEADER_2>|S|<HEADER_VALUE2>")
        self.assert_reply("10000010c3e4d0462|NUA|EA|An+empty+user+is+not+"
                          "allowed")

    def test_notify_user_auth_with_generic_exception(self):
        # Testing other errors
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|NUA|S|user2|S|remote_password|S|"
                          "<CLIENT_PRINCIPAL>|S|<HEADER_1>|S|<HEADER_VALUE1>|"
                          "S|<HEADER_2>|S|<HEADER_VALUE2>")

        self.assert_reply("10000010c3e4d0462|NUA|E|Error+while+notifying")

    def test_malformed_notify_user_auth(self):
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|NUA|S|")
        self.assert_caught_exception("Token not found while parsing NUA "
                                     "request")

        self.send_request("10000010c3e4d0462|NUA|S1|user")
        self.assert_caught_exception("Unknown type 'S1' found while parsing "
                                     "NUA request")

    def test_notify_new_session(self):
        self.do_init_and_skip()
        self.send_request("10000020b3e6d0462|NNS|S|<user name>|S|"
                          "<session ID>|S|<context prop name 1>|S|"
                          "<context prop 1 value>|S|<context prop name N>|S|"
                          "<context prop N value>")

        self.assert_reply("10000020b3e6d0462|NNS|V")
        self.assertDictEqual({"user": "<user name>",
                              "sessionId": "<session ID>",
                              "client_context": {"<context prop name 1>":
                                                 "<context prop 1 value>",
                                                 "<context prop name N>":
                                                 "<context prop N value>"}},
                             self.collector)

    def test_notify_new_session_with_exception(self):
        # Testing CreditError
        self.do_init_and_skip()
        self.send_request("10000020b3e6d0462|NNS|S|user1|S|<session ID>|S|"
                          "<context prop name 1>|S|<context prop 1 value>|S|"
                          "<context prop name N>|S|<context prop N value>")
        self.assert_reply("10000020b3e6d0462|NNS|EC|CreditsError|9|User+not+"
                          "allowed")

    def test_notify_new_session_with_notification_exception(self):
        # Testing NotificationError
        self.do_init_and_skip()
        self.send_request("10000020b3e6d0462|NNS|S|user2|S|<session ID>|S|"
                          "<context prop name 1>|S|<context prop 1 value>|S|"
                          "<context prop name N>|S|<context prop N value>")
        self.assert_reply("10000020b3e6d0462|NNS|EN|NotificationError")

    def test_notify_new_session_with_conflicting_session_exception(self):
        # Testing ConflictingSessionError
        self.do_init_and_skip()
        self.send_request("10000020b3e6d0462|NNS|S|user3|S|<session ID>|S|"
                          "<context prop name 1>|S|<context prop 1 value>|S|"
                          "<context prop name N>|S|<context prop N value>")
        self.assert_reply("10000020b3e6d0462|NNS|EX|ConflictingSessionError|"
                          "11|clientErrorMsg|conflictingSessionID")

    def test_notify_new_session_with_generic_exception(self):
        # Testing generic Error
        self.do_init_and_skip()
        self.send_request("10000020b3e6d0462|NNS|S|user4|S|<session ID>|S|"
                          "<context prop name 1>|S|<context prop 1 value>|S|"
                          "<context prop name N>|S|<context prop N value>")
        self.assert_reply("10000020b3e6d0462|NNS|E|Error+while+notifying")

    def test_malformed_notify_new_session(self):
        self.do_init_and_skip()
        self.send_request("10000020b3e6d0462|NNS|S|")
        self.assert_caught_exception("Token not found while parsing NNS "
                                     "request")

        self.send_request("10000020b3e6d0462|NNS|S1|user4")
        self.assert_caught_exception("Unknown type 'S1' found while parsing "
                                     "NNS request")

    def test_notify_session_close(self):
        self.do_init_and_skip()
        self.send_request("20000010c3e4d0462|NSC|S|S8f3da29cfc463220T5454537")
        self.assert_reply("20000010c3e4d0462|NSC|V")
        self.assertDictEqual({'sessionId': 'S8f3da29cfc463220T5454537'},
                             self.collector)

    def test_notify_session_close_with_notification_exception(self):
        # Testing NotificationError provoked by an empty SessionId ()
        self.do_init_and_skip()
        self.send_request("20000010c3e4d0462|NSC|S|$")
        self.assert_reply("20000010c3e4d0462|NSC|EN|Empty+SessionId")
        self.assertDictEqual({}, self.collector)

    def test_notify_session_close_with_generic_exception(self):
        # Testing generic Error provoked by null SessionId (#)
        self.do_init_and_skip()
        self.send_request("20000010c3e4d0462|NSC|S|#")
        self.assertDictEqual({}, self.collector)
        self.assert_reply("20000010c3e4d0462|NSC|E|NULL+SessionId")

    def test_malformed_notify_session_close(self):
        self.do_init_and_skip()
        self.send_request("20000010c3e4d0462|NSC|S|")
        self.assert_caught_exception("Token not found while parsing NSC "
                                     "request")

        self.send_request("20000010c3e4d0462|NSC|S1|user4")
        self.assert_caught_exception("Unknown type 'S1' found while parsing "
                                     "NSC request")

    def test_get_items(self):
        self.do_init_and_skip()
        self.send_request("50000010c3e4d0462|GIS|S|user1|S|nasdaq100_AA_AL+"
                          "abc|S|S8f3da29cfc463220T5454537")
        self.assert_reply("50000010c3e4d0462|GIS|S|nasdaq100_AA_AL|S|abc")
        self.assertDictEqual({"user": "user1",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL abc"}, self.collector)

    def test_get_items_with_no_returned_items(self):
        self.do_init_and_skip()
        self.send_request("50000010c3e4d0462|GIS|S|user1|S|no+items|S|"
                          "S8f3da29cfc463220T5454537")
        self.assert_reply("50000010c3e4d0462|GIS")
        self.assertDictEqual({"user": "user1",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "no items"}, self.collector)

    def test_get_items_with_items_exception(self):
        # Testing ItemsError Error provoked by an empty user ()
        self.do_init_and_skip()
        self.send_request("50000010c3e4d0462|GIS|S|$|S|nasdaq100_AA_AL+abc|S|"
                          "S8f3da29cfc463220T5454537")
        self.assert_reply("50000010c3e4d0462|GIS|EI|Empty+User")
        self.assertDictEqual({"user": "",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL abc"}, self.collector)

    def test_get_items_with_generic_exception(self):
        # Testing generic Error provoked by a null user (#)
        self.do_init_and_skip()
        self.send_request("50000010c3e4d0462|GIS|S|#|S|nasdaq100_AA_AL+abc|S|"
                          "S8f3da29cfc463220T5454537")

        self.assert_reply("50000010c3e4d0462|GIS|E|NULL+User")
        self.assertDictEqual({"user": None,
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL abc"},
                             self.collector)

    def test_invalid_get_items(self):
        self.do_init_and_skip()
        self.send_request("50000010c3e4d0462|GIS|S|user1|S|get+invalid|S|"
                          "S8f3da29cfc463220T5454537")
        self.assert_caught_exception("Unknown error while url-encoding string")

    def test_malformed_get_items(self):
        self.do_init_and_skip()
        self.send_request("50000010c3e4d0462|GIS|S|")
        self.assert_caught_exception("Token not found while parsing GIS "
                                     "request")

        self.send_request("50000010c3e4d0462|GIS|S1|nasdaq100_AA_AL")
        self.assert_caught_exception("Unknown type 'S1' found while parsing "
                                     "GIS request")

    def test_get_schema(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GSC|S|user1|S|nasdaq100_AA_AL+"
                          "abc|S|short1+short2|S|S8f3da29cfc463220T5454537")
        self.assert_reply("70000010c3e4d0462|GSC|S|short1|S|short2")
        self.assertDictEqual({"user": "user1",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL abc",
                              "schema": "short1 short2"}, self.collector)

    def test_get_schema_with_no_returned_schema(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GSC|S|user1|S|nasdaq100_AA_AL+"
                          "abc|S|no+schema|S|S8f3da29cfc463220T5454537")
        self.assert_reply("70000010c3e4d0462|GSC")
        self.assertDictEqual({"user": "user1",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL abc",
                              "schema": "no schema"}, self.collector)

    def test_get_schema_with_items_exception(self):
        # Testing ItemsError provoked by null user (#)
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GSC|S|#|S|nasdaq100_AA_AL+abc|S|"
                          "short1+short2|S|S8f3da29cfc463220T5454537")
        self.assert_reply("70000010c3e4d0462|GSC|EI|NULL+User")
        self.assertDictEqual({"user": None,
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL abc",
                              "schema": "short1 short2"}, self.collector)

    def test_get_schema_with_schema_exception(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GSC|S|user1|S|nasdaq100_AA_AL|S|"
                          "shortA|S|S8f3da29cfc463220T5454537")
        self.assert_reply("70000010c3e4d0462|GSC|ES|SchemaError")
        self.assertDictEqual({"user": "user1",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL",
                              "schema": "shortA"}, self.collector)

    def test_get_schema_with_generic_exception(self):
        # Testing ItemsEerror provoked by an empty user
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GSC|S|$|S|nasdaq100_AA_AL+abc|S|"
                          "short1+short2|S|S8f3da29cfc463220T5454537")
        self.assert_reply("70000010c3e4d0462|GSC|E|Empty+User")
        self.assertDictEqual({"user": "",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL abc",
                              "schema": "short1 short2"}, self.collector)

    def test_invalid_get_schema(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GSC|S|user1|S|nasdaq100_AA_AL|S|"
                          "get+invalid|S|S8f3da29cfc463220T5454537")
        self.assert_caught_exception("Unknown error while url-encoding string")

    def test_malformed_get_schema(self):
        self.do_init_and_skip()

        # Missing token.
        self.send_request("70000010c3e4d0462|GSC|S|")
        self.assert_caught_exception("Token not found while parsing GSC "
                                     "request")

        # Wrong token type.
        self.send_request("70000010c3e4d0462|GSC|S1|nasdaq100_AA_AL")
        self.assert_caught_exception("Unknown type 'S1' found while parsing "
                                     "GSC request")

        # No fields specifications.
        self.send_request("70000010c3e4d0462|GSC|S|user1|S|nasdaq100_AA_AL+"
                          "abc|S|S8f3da29cfc463220T5454537")
        self.assert_caught_exception("Token not found while parsing GSC "
                                     "request")

    def test_get_item_data(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GIT|S|item_1|S|item_2|S|item_3")
        self.assert_reply("70000010c3e4d0462|GIT|I|10|D|4.5|M|MD|I|20|D|7.3"
                          "|M|RMC|I|0|D|0.0|M|$")

    def test_get_item_data_with_no_specified_item(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GIT")
        self.assert_reply("70000010c3e4d0462|GIT")

    def test_get_item_data_with_exception(self):
        # Testing generic Error provoked by item_4
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GIT|S|item_1|S|item_2|S|item_4")
        self.assert_reply("70000010c3e4d0462|GIT|E|Error+for+provided+item")

    def test_invalid_get_item_data(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GIT|S|get+invalid")
        self.assert_caught_exception("Not an int value: 'Wrong Distinct "
                                     "Snapshot Length Type'")

    def test_malformed_get_item_data(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GIT|S|")
        self.assert_caught_exception("Token not found while parsing GIT "
                                     "request")

        self.send_request("70000010c3e4d0462|GIT|S1|item_name")
        self.assert_caught_exception("Unknown type 'S1' found while parsing "
                                     "GIT request")

    def test_get_user_item_data(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GUI|S|user1|S|item_1|S|item_2|S|"
                          "item_3")
        self.assert_reply("70000010c3e4d0462|GUI|I|30|D|170.5|M|RMDC|I|40|D|"
                          "27.3|M|RM|I|0|D|0.0|M|$")

    def test_get_user_item_data_with_no_specified_items(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GUI|S|user1")
        self.assert_reply("70000010c3e4d0462|GUI")

    def test_get_user_item_data_with_exception(self):
        # Testing generic Error provoked by user2
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GUI|S|user2|S|item_1|S|item_2|S|"
                          "item_4")
        self.assert_reply("70000010c3e4d0462|GUI|E|No+user+allowed")

    def test_invalid_get_user_item_data(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GUI|S|user1|S|get+invalid")
        self.assert_caught_exception("Not an int value: 'Wrong Allowed Buffer"
                                     " Size'")

    def test_malformed_get_user_item_data(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GUI")
        self.assert_caught_exception("Token not found while parsing GUI "
                                     "request")

        self.send_request("70000010c3e4d0462|GUI|S|")
        self.assert_caught_exception("Token not found while parsing GUI "
                                     "request")

        self.send_request("70000010c3e4d0462|GUI|S1|user2")
        self.assert_caught_exception("Unknown type 'S1' found while parsing"
                                     " GUI request")

    def test_notify_user_message(self):
        self.do_init_and_skip()
        self.send_request("d0000010c3e4d0462|NUM|S|user1|S|"
                          "S8f3da29cfc463220T5454537|S|stop+logging")
        self.assert_reply("d0000010c3e4d0462|NUM|V")
        self.assertDictEqual({"user": "user1",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "message": "stop logging"},
                             self.collector)

    def test_notify_user_message_with_notification_exception(self):
        self.do_init_and_skip()
        self.send_request("d0000010c3e4d0462|NUM|S|user2|S|"
                          "S8f3da29cfc463220T5454537|S|stop+logging")
        self.assert_reply("d0000010c3e4d0462|NUM|EN|NotificationError")

    def test_notify_user_message_with_credits_exception(self):
        self.do_init_and_skip()
        self.send_request("d0000010c3e4d0462|NUM|S|user3|S|"
                          "S8f3da29cfc463220T5454537|S|stop+logging")
        self.assert_reply("d0000010c3e4d0462|NUM|EC|CreditsError|4|"
                          "clientErrorMsg")

    def test_notify_user_message_with_generic_exception(self):
        self.do_init_and_skip()
        self.send_request("d0000010c3e4d0462|NUM|S|user4|S|"
                          "S8f3da29cfc463220T5454537|S|stop+logging")
        self.assert_reply("d0000010c3e4d0462|NUM|E|Exception")

    def test_malformed_notify_user_message(self):
        self.do_init_and_skip()
        self.send_request("d0000010c3e4d0462|NUM|S|")
        self.assert_caught_exception("Token not found while parsing NUM "
                                     "request")

        self.send_request("d0000010c3e4d0462|NUM|S1|user4")
        self.assert_caught_exception("Unknown type 'S1' found while parsing "
                                     "NUM request")

    def test_notify_new_tables(self):
        self.do_init_and_skip()
        self.send_request("f0000010c3e4d0462|NNT|S|#|S|"
                          "S8f3da29cfc463220T5454537|I|1|M|M|S|"
                          "nasdaq100_AA_AL|S|short|I|1|I|5|S|#|I|2|M|D|S|"
                          "nasdaq100_AA_AL|S|medium|I|4|I|3|S|selector")

        self.assert_reply("f0000010c3e4d0462|NNT|V")
        self.assertDictEqual({"user": None,
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "tableInfos": [TableInfo(win_index=1,
                                                       mode=Mode.MERGE,
                                                       group="nasdaq100_AA_AL",
                                                       schema="short",
                                                       first_idx=1,
                                                       last_idx=5),
                                             TableInfo(win_index=2,
                                                       mode=Mode.DISTINCT,
                                                       group="nasdaq100_AA_AL",
                                                       schema="medium",
                                                       first_idx=4,
                                                       last_idx=3,
                                                       selector='selector')]},
                             self.collector)

    def test_notify_new_tables_with_null_mode(self):
        self.do_init_and_skip()
        self.send_request("f0000010c3e4d0462|NNT|S|#|S|"
                          "S8f3da29cfc463220T5454537|I|1|M|#|S|"
                          "nasdaq100_AA_AL|S|short|I|1|I|5|S|#|I|2|M|D|S|"
                          "nasdaq100_AA_AL|S|medium|I|4|I|3|S|selector")
        self.assert_reply("f0000010c3e4d0462|NNT|V")
        self.assertDictEqual({"user": None,
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "tableInfos": [TableInfo(win_index=1,
                                                       mode=None,
                                                       group="nasdaq100_AA_AL",
                                                       schema="short",
                                                       first_idx=1,
                                                       last_idx=5),
                                             TableInfo(win_index=2,
                                                       mode=Mode.DISTINCT,
                                                       group="nasdaq100_AA_AL",
                                                       schema="medium",
                                                       first_idx=4,
                                                       last_idx=3,
                                                       selector="selector")]},
                             self.collector)

    def test_notify_new_tables_with_empty_mode(self):
        self.do_init_and_skip()
        self.send_request("f0000010c3e4d0462|NNT|S|#|S|"
                          "S8f3da29cfc463220T5454537|I|1|M|$|S|"
                          "nasdaq100_AA_AL|S|short|I|1|I|5|S|#|I|2|M|D|S|"
                          "nasdaq100_AA_AL|S|medium|I|4|I|3|S|selector")
        self.assert_reply("f0000010c3e4d0462|NNT|V")
        self.assertDictEqual({"user": None,
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "tableInfos": [TableInfo(win_index=1,
                                                       mode=None,
                                                       group="nasdaq100_AA_AL",
                                                       schema="short",
                                                       first_idx=1,
                                                       last_idx=5),
                                             TableInfo(win_index=2,
                                                       mode=Mode.DISTINCT,
                                                       group="nasdaq100_AA_AL",
                                                       schema="medium",
                                                       first_idx=4,
                                                       last_idx=3,
                                                       selector="selector")]},
                             self.collector)

    def test_notify_new_tables_with_credits_exception(self):
        self.do_init_and_skip()
        self.send_request("f0000010c3e4d0462|NNT|S|user1|S|"
                          "S8f3da29cfc463220T5454537|I|1|M|M|S|"
                          "nasdaq100_AA_AL|S|short|I|1|I|5|S|#")
        self.assert_reply("f0000010c3e4d0462|NNT|EC|CreditsError|10|"
                          "clientErrorMsg")

    def test_notify_new_tables_with_notification_exception(self):
        self.do_init_and_skip()
        self.send_request("f0000010c3e4d0462|NNT|S|user2|S|"
                          "S8f3da29cfc463220T5454537|I|1|M|M|S|"
                          "nasdaq100_AA_AL|S|short|I|1|I|5|S|#")
        self.assert_reply("f0000010c3e4d0462|NNT|EN|NotificationError")

    def test_notify_new_tables_with_exception(self):
        self.do_init_and_skip()
        self.send_request("f0000010c3e4d0462|NNT|S|user3|S|"
                          "S8f3da29cfc463220T5454537|I|1|M|M|S|"
                          "nasdaq100_AA_AL|S|short|I|1|I|5|S|#")
        self.assert_reply("f0000010c3e4d0462|NNT|E|Exception")

    def test_malformed_notify_new_tables(self):
        self.do_init_and_skip()
        self.send_request("f0000010c3e4d0462|NNT|S|#|S")
        self.assert_caught_exception("Token not found while parsing NNT "
                                     "request")

        self.send_request("f0000010c3e4d0462|NNT|S1|#")
        self.assert_caught_exception("Unknown type 'S1' found while parsing "
                                     "NNT request")

    def test_notify_tables_close(self):
        self.do_init_and_skip()
        self.send_request("f0000010c3e4d0462|NTC|S|S8f3da29cfc463220T5454537|"
                          "I|1|M|M|S|nasdaq100_AA_AL|S|short|I|1|I|5|S|#")
        self.assert_reply("f0000010c3e4d0462|NTC|V")
        self.assertDictEqual({"sessionId": "S8f3da29cfc463220T5454537",
                              "tableInfos": [TableInfo(win_index=1,
                                                       mode=Mode.MERGE,
                                                       group="nasdaq100_AA_AL",
                                                       schema="short",
                                                       first_idx=1,
                                                       last_idx=5)]},
                             self.collector)

    def test_notify_tables_close_with_no_table_info(self):
        self.do_init_and_skip()
        self.send_request("f0000010c3e4d0462|NTC|S|S8f3da29cfc463220T5454537")
        self.assert_reply("f0000010c3e4d0462|NTC|V")
        self.assertDictEqual({"sessionId": "S8f3da29cfc463220T5454537",
                              "tableInfos": []}, self.collector)

    def test_notify_tables_close_with_notification_exception(self):
        self.do_init_and_skip()
        self.send_request("f0000010c3e4d0462|NTC|S|S8f3da29cfc463220T5454539|"
                          "I|1|M|M|S|nasdaq100_AA_AL|S|short|I|1|I|5|S|#")
        self.assert_reply("f0000010c3e4d0462|NTC|EN|NotificationError")

    def test_notify_tables_close_with_generic_exception(self):
        self.do_init_and_skip()
        self.send_request("f0000010c3e4d0462|NTC|S|S8f3da29cfc463220T5454540|"
                          "I|1|M|M|S|nasdaq100_AA_AL|S|short|I|1|I|5|S|#")
        self.assert_reply("f0000010c3e4d0462|NTC|E|Exception")

    def test_malformed_notify_tables_close(self):
        self.do_init_and_skip()
        # A "S" token type is expected.
        self.send_request("f0000010c3e4d0462|NTC|S1|#")
        self.assert_caught_exception("Unknown type 'S1' found while parsing "
                                     "NTC request")

        # A "I" token type is expected.
        self.send_request("f0000010c3e4d0462|NTC|S|#|S1")
        self.assert_caught_exception("Unknown type 'S1' found while parsing "
                                     "NTC request")

    def test_notify_device_access(self):
        self.do_init_and_skip()
        self.send_request("b00000147c9bc4c74|MDA|S|user1"
                          "|S|S8f3da29cfc463220T5454537|P|A|S|"
                          "com.lightstreamer.demo.ios.stocklistdemo|S|"
                          "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                          "24cff14a9906f1")
        self.assert_reply("b00000147c9bc4c74|MDA|V")
        self.assertDictEqual({"user": "user1",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "mpnDeviceInfo":
                              MpnDeviceInfo(
                                  platform_type=MpnPlatformType.APPLE,
                                  application_id="com.lightstreamer"
                                                 ".demo.ios."
                                                 "stocklistdemo",
                                  device_token="f780e9d8ffc86a5ec9a"
                                               "329e7745aa8fb3a1ecc"
                                               "e77c09e202ec24cff14"
                                               "a9906f1")}, self.collector)

    def test_notify_device_access_with_notification_exception(self):
        self.do_init_and_skip()
        self.send_request("b00000147c9bc4c74|MDA|S|#"
                          "|S|S8f3da29cfc463220T5454537|P|A|S|"
                          "com.lightstreamer.demo.ios.stocklistdemo|S|"
                          "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                          "24cff14a9906f1")
        self.assert_reply("b00000147c9bc4c74|MDA|EN|NotificationError")

    def test_notify_device_access_with_credits_exception(self):
        self.do_init_and_skip()
        self.send_request("b00000147c9bc4c74|MDA|S|$"
                          "|S|S8f3da29cfc463220T5454537|P|A|S|"
                          "com.lightstreamer.demo.ios.stocklistdemo|S|"
                          "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                          "24cff14a9906f1")
        self.assert_reply("b00000147c9bc4c74|MDA|EC|CreditsError|10|"
                          "clientErrorMsg")

    def test_malformed_notify_device_access(self):
        self.do_init_and_skip()
        self.send_request("b00000147c9bc4c74|MDA|S|#"
                          "|S|S8f3da29cfc463220T5454537|P")
        self.assert_caught_exception("Token not found while parsing MDA "
                                     "request")

        self.send_request("b00000147c9bc4c74|MDA|S1|#"
                          "|S|S8f3da29cfc463220T5454537|P")
        self.assert_caught_exception("Unknown type 'S1' found while parsing"
                                     " MDA request")

        self.send_request("b00000147c9bc4c74|MDA|S|#"
                          "|S|S8f3da29cfc463220T5454537|P|Q")
        self.assert_caught_exception("Unknown platform type 'Q' while parsing "
                                     "MDA request")

    def test_notify_device_access_with_generic_exception(self):
        self.do_init_and_skip()
        self.send_request("b00000147c9bc4c74|MDA|S|user2"
                          "|S|S8f3da29cfc463220T5454537|P|A|S|"
                          "com.lightstreamer.demo.ios.stocklistdemo|S|"
                          "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                          "24cff14a9906f1")
        self.assert_reply("b00000147c9bc4c74|MDA|E|Exception")

    def test_notify_mpn_subscription_activation_apn(self):
        self.do_init_and_skip()
        request = ("c00000147c9bc4c74|MSA|S|user1|S|Sc4a1769b6bb83a4aT2852044|"
                   # I|<win. index>|M|<pub. mode>|S|<item_name group>|S|
                   # <field schema>|
                   "I|1|M|M|S|item4+item19|S|stock_name+last_price+time|"
                   # I|<first item_name idx.>|I|<last item_name idx.>|
                   "I|1|I|2|"
                   # P|A|S|<application ID>|S|<device token>|S|<trigger>|
                   "P|A|S|com.lightstreamer.demo.ios.stocklistdemo|S|"
                   "f74d8ffc5ee7cb31749a329a8f9202867c0a9906e80ee7f9eeccca1f24"
                   "c5aaf1|S|"
                   "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+1000.0|"
                   # S|<notification_format
                   "S|%7B%22aps%22%3A%7B%22alert%22%3A%22%24%7Bmessage%7D%22"
                   "%2C%22badge%22%3A%22AUTO%22%7D%2C%22acme2%22%3A%5B%22"
                   "%24%7Btag1%7D%22%2C%22%24%7Btag2%7D%22%5D%7D")

        self.send_request(request)
        self.assert_reply("c00000147c9bc4c74|MSA|V")

        device = MpnDeviceInfo(
            platform_type=MpnPlatformType.APPLE,
            application_id="com.lightstreamer.demo.ios.stocklistdemo",
            device_token="f74d8ffc5ee7cb31749a329a8f9202867c0a9906e8"
                         "0ee7f9eeccca1f24c5aaf1")

        subscription = MpnSubscriptionInfo(
            device=device,
            notification_format="{\"aps\":{\"alert\":\"${message}\","
                                "\"badge\":\"AUTO\"},\"acme2\":"
                                "[\"${tag1}\",\"${tag2}\"]}",
            trigger="Double.parseDouble(${last_price}) > 1000.0")

        table_info = TableInfo(win_index=1, mode=Mode.MERGE,
                               group="item4 item19",
                               schema="stock_name last_price time",
                               first_idx=1, last_idx=2)

        expected_dict = {"user": "user1",
                         "sessionId": "Sc4a1769b6bb83a4aT2852044",
                         "table": table_info,
                         "mpn_subscription": subscription}

        self.assertDictEqual(expected_dict, self.collector)

    def test_notify_mpn_subscription_activation_apn_with_ne_exception(self):
        # Expect a NotificationError because NULL user is specified:
        # "c00000147c9bc4c74|MSA|S|#|..."
        self.do_init_and_skip()
        self.send_request("c00000147c9bc4c74|MSA|S|#|S|"
                          "Sc4a1769b6bb83a4aT2852044|I|1|M|M|S|item4+item19|"
                          "S|stock_name+last_price+time|I|1|I|2|P|A|S|"
                          "com.lightstreamer.demo.ios.stocklistdemo|S|"
                          "f74d8ffc5ee7cb31749a329a8f9202867c0a9906e80ee7f9ee"
                          "ccca1f24c5aaf1|S|"
                          "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+"
                          "1000.0|S|%7B%22aps%22%3A%7B%22alert%22%3A%22"
                          "%24%7Bmessage%7D%22%2C%22badge%22%3A%22AUTO%22"
                          "%7D%2C%22acme2%22%3A%5B%22%24%7Btag1%7D%22%2C%22"
                          "%24%7Btag2%7D%22%5D%7D")
        self.assert_reply("c00000147c9bc4c74|MSA|EN|NotificationError")

    def test_notify_mpn_subscription_activation_apn_with_ce_exception(self):
        # Expect a NotificationError because EMPTY user is specified:
        # "c00000147c9bc4c74|MSA|S|$|..."
        self.do_init_and_skip()
        self.send_request("c00000147c9bc4c74|MSA|S|$|S|"
                          "Sc4a1769b6bb83a4aT2852044|I|1|M|M|S|item4+item19|"
                          "S|stock_name+last_price+time|I|1|I|2|P|A|S|"
                          "com.lightstreamer.demo.ios.stocklistdemo|S|"
                          "f74d8ffc5ee7cb31749a329a8f9202867c0a9906e80ee7f9ee"
                          "ccca1f24c5aaf1|S|"
                          "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+"
                          "1000.0|S|%7B%22aps%22%3A%7B%22alert%22%3A%22"
                          "%24%7Bmessage%7D%22%2C%22badge%22%3A%22AUTO%22"
                          "%7D%2C%22acme2%22%3A%5B%22%24%7Btag1%7D%22%2C%22"
                          "%24%7Btag2%7D%22%5D%7D")
        self.assert_reply("c00000147c9bc4c74|MSA|EC|CreditsError|10|"
                          "clientErrorMsg")

    def test_notify_mpn_subscription_activation_apn_with_exception(self):
        # Expect a NotificationError because "user2" user is specified:
        # "c00000147c9bc4c74|MSA|S|user2|..."
        self.do_init_and_skip()
        self.send_request("c00000147c9bc4c74|MSA|S|user2|S|"
                          "Sc4a1769b6bb83a4aT2852044|I|1|M|M|S|item4+item19|"
                          "S|stock_name+last_price+time|I|1|I|2|P|A|S|"
                          "com.lightstreamer.demo.ios.stocklistdemo|S|"
                          "f74d8ffc5ee7cb31749a329a8f9202867c0a9906e80ee7f9ee"
                          "ccca1f24c5aaf1|S|"
                          "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+"
                          "1000.0|S|%7B%22aps%22%3A%7B%22alert%22%3A%22"
                          "%24%7Bmessage%7D%22%2C%22badge%22%3A%22AUTO%22"
                          "%7D%2C%22acme2%22%3A%5B%22%24%7Btag1%7D%22%2C%22"
                          "%24%7Btag2%7D%22%5D%7D")
        self.assert_reply("c00000147c9bc4c74|MSA|E|Exception")

    def test_notify_mpn_subscription_activation_gcm(self):
        self.do_init_and_skip()
        request = ("c00000147c9bc4c74|MSA|S|user1|S|Sc4a1769b6bb83a4aT2852044|"
                   # I|<win. index>|M|<pub. mode>|S|<item_name group>|S|
                   # <field schema>|
                   "I|1|M|M|S|item4+item19|S|stock_name+last_price+time|"
                   # I|<first item_name idx.>|I|<last item_name idx.>|
                   "I|1|I|2|"
                   # P|G|S|<application ID>|S|<device token>|S|<trigger>|
                   "P|G|S|com.lightstreamer.demo.android.stocklistdemo|S|"
                   "2082055669|S|"
                   "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+1000.0|"
                   # S|notification_format
                   "S|%7B%22data%22%3A%7B%22score%22%3A%22%24%7Bscore%7D%22"
                   "%2C%22time%22%3A%22%24%7Btime%7D%22%7D%7D")
        self.send_request(request)
        self.assert_reply("c00000147c9bc4c74|MSA|V")

        table_info = TableInfo(win_index=1, mode=Mode.MERGE,
                               group="item4 item19",
                               schema="stock_name last_price time",
                               first_idx=1, last_idx=2)
        device = MpnDeviceInfo(
            platform_type=MpnPlatformType.GOOGLE,
            application_id="com.lightstreamer.demo.android.stocklistdemo",
            device_token="2082055669")

        subscription = MpnSubscriptionInfo(
            device=device,
            notification_format="{\"data\":{\"score\":\"${score}\","
                                "\"time\":\"${time}\"}}",
            trigger="Double.parseDouble(${last_price}) > 1000.0")

        expected_dict = {"user": "user1",
                         "sessionId": "Sc4a1769b6bb83a4aT2852044",
                         "table": table_info,
                         "mpn_subscription": subscription}
        self.assertDictEqual(expected_dict, self.collector)

    def test_notify_mpn_subscription_activation_google_with_ne_exception(self):
        self.do_init_and_skip()
        self.send_request("c00000147cac69643|MSA|S|#|S|"
                          "S401e2449d3b79feT1213883|I|1|M|M|S|item4+item19|S|"
                          "stock_name+last_price+time|I|1|I|2|P|G|S|"
                          "com.lightstreamer.demo.android.stocklistdemo|S|"
                          "2082055669|S|"
                          "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+"
                          "1000.0|S|%7B%22data%22%3A%7B%22score%22%3A%22"
                          "%24%7Bscore%7D%22%2C%22time%22%3A%22"
                          "%24%7Btime%7D%22%7D%7D")
        self.assert_reply("c00000147cac69643|MSA|EN|NotificationError")

    def test_notify_mpn_subscription_activation_google_with_ce_exception(self):
        self.do_init_and_skip()
        self.send_request("c00000147cac69643|MSA|S|$|S|"
                          "S401e2449d3b79feT1213883|I|1|M|M|S|item4+item19|S|"
                          "stock_name+last_price+time|I|1|I|2|P|G|S|"
                          "com.lightstreamer.demo.android.stocklistdemo|S|"
                          "2082055669|S|"
                          "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+"
                          "1000.0|S|%7B%22data%22%3A%7B%22score%22%3A%22"
                          "%24%7Bscore%7D%22%2C%22time%22%3A%22"
                          "%24%7Btime%7D%22%7D%7D")
        self.assert_reply("c00000147cac69643|MSA|EC|CreditsError|10|"
                          "clientErrorMsg")

    def test_notify_mpn_subscription_activation_google_with_exception(self):
        self.do_init_and_skip()
        self.send_request("c00000147cac69643|MSA|S|user2|S"
                          "|S401e2449d3b79feT1213883|I|1|M|M|S|item4+item19"
                          "|S|stock_name+last_price+time|I|1|I|2|P|G|S|"
                          "com.lightstreamer.demo.android.stocklistdemo|S|"
                          "2082055669|S|"
                          "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+"
                          "1000.0|S|%7B%22data%22%3A%7B%22score%22%3A%22"
                          "%24%7Bscore%7D%22%2C%22time%22%3A%22"
                          "%24%7Btime%7D%22%7D%7D")
        self.assert_reply("c00000147cac69643|MSA|E|Exception")

    def test_malformed_mpn_subscription_activation(self):
        self.do_init_and_skip()
        # Invalid "P" token as mpn platform type

        request = ("c00000147c9bc4c74|MSA|S|user1|S|Sc4a1769b6bb83a4aT2852044|"
                   # I|<win. index>|M|<pub. mode>|S|<item_name group>|S|
                   # <field schema>|
                   "I|1|M|M|S|item4+item19|S|stock_name+last_price+time|"
                   # I|<first item_name idx.>|I|<last item_name idx.>|
                   "I|1|I|2|"
                   # P|G|S|<application ID>|S|<device token>|S|<trigger>|
                   "P|R|")
        self.send_request(request)
        self.assert_caught_exception("Unknown platform type 'R' while parsing "
                                     "MSA request")

        # Missing next token to P
        request = ("c00000147c9bc4c74|MSA|S|user1|S|Sc4a1769b6bb83a4aT2852044|"
                   # I|<win. index>|M|<pub. mode>|S|<item_name group>|S|
                   # <field schema>|
                   "I|1|M|M|S|item4+item19|S|stock_name+last_price+time|"
                   # I|<first item_name idx.>|I|<last item_name idx.>|
                   "I|1|I|2|"
                   # P|G|S|<application ID>|S|<device token>|S|<trigger>|
                   "P|")
        self.send_request(request)
        self.assert_caught_exception("Token not found while parsing MSA "
                                     "request")

        # Invalid next literal token to "P"
        request = ("c00000147c9bc4c74|MSA|S|user1|S|Sc4a1769b6bb83a4aT2852044|"
                   # I|<win. index>|M|<pub. mode>|S|<item_name group>|S|
                   # <field schema>|
                   "I|1|M|M|S|item4+item19|S|stock_name+last_price+time|"
                   # I|<first item_name idx.>|I|<last item_name idx.>|
                   "I|1|I|R|"
                   # P|G|S|<application ID>|S|<device token>|S|<trigger>|
                   "P|G")
        self.send_request(request)
        self.assert_caught_exception("An unexpected exception caught while "
                                     "parsing MSA request")

        # Invalid "H" token as mode type
        request = ("c00000147c9bc4c74|MSA|S|user1|S|Sc4a1769b6bb83a4aT2852044|"
                   # I|<win. index>|M|<pub. mode>|S|<item_name group>|S|
                   # <field schema>|
                   "I|1|M|H|S|item4+item19|S|stock_name+last_price+time|"
                   # I|<first item_name idx.>|I|<last item_name idx.>|
                   "I|1|I|2|"
                   # P|G|S|<application ID>|S|<device token>|S|<trigger>|
                   "P|G|S|com.lightstreamer.demo.android.stocklistdemo|S|"
                   "2082055669|S|"
                   "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+1000.0|"
                   # S|notification_format
                   "S|%7B%22data%22%3A%7B%22score%22%3A%22%24%7Bscore%7D%22"
                   "%2C%22time%22%3A%22%24%7Btime%7D%22%7D%7D")
        self.send_request(request)
        self.assert_caught_exception("Unknown mode 'H' found while parsing "
                                     "MSA request")

    def test_notify_mpn_device_token_change(self):
        self.do_init_and_skip()
        self.send_request("3700000147c9bc4c74|MDC|S|user1"
                          "|S|S8f3da29cfc463220T5454537|P|A|S|"
                          "com.lightstreamer.demo.ios.stocklistdemo|S|"
                          "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                          "24cff14a9906f1|S|0849781a0afe0311f58bbfee1fcde031b"
                          "fc56635c89c566dda3c6708fd893549")
        self.assert_reply("3700000147c9bc4c74|MDC|V")
        device = MpnDeviceInfo(
            platform_type=MpnPlatformType.APPLE,
            application_id="com.lightstreamer.demo.ios.stocklistdemo",
            device_token="f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec2"
                         "4cff14a9906f1")

        expected_dict = {"user": "user1",
                         "sessionId": "S8f3da29cfc463220T5454537",
                         "mpnDeviceInfo": device,
                         "newDeviceToken": "0849781a0afe0311f58bbfee1fcde031b"
                                           "fc56635c89c566dda3c6708fd893549"}
        self.assertDictEqual(expected_dict, self.collector)

    def test_notify_mpn_device_token_change_with_null_platform_type(self):
        self.do_init_and_skip()
        self.send_request("3700000147c9bc4c74|MDC|S|user1"
                          "|S|S8f3da29cfc463220T5454537|P|#|S|"
                          "com.lightstreamer.demo.ios.stocklistdemo|S|"
                          "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                          "24cff14a9906f1|S|0849781a0afe0311f58bbfee1fcde031b"
                          "fc56635c89c566dda3c6708fd893549")
        self.assert_reply("3700000147c9bc4c74|MDC|V")
        device = MpnDeviceInfo(platform_type=None,
                               application_id="com.lightstreamer.demo.ios."
                                              "stocklistdemo",
                               device_token="f780e9d8ffc86a5ec9a329e7745aa8fb"
                                            "3a1ecce77c""09e202ec24cff14a9906""f1")
        expected_dict = {"user": "user1",
                         "sessionId": "S8f3da29cfc463220T5454537",
                         "mpnDeviceInfo": device,
                         "newDeviceToken": "0849781a0afe0311f58bbfee1fcde031b"
                                           "fc56635c89c566dda3c6708fd893549"}
        self.assertDictEqual(expected_dict, self.collector)

    def test_notify_mpn_device_token_change_with_empty_platform_type(self):
        self.do_init_and_skip()
        self.send_request("3700000147c9bc4c74|MDC|S|user1"
                          "|S|S8f3da29cfc463220T5454537|P|$|S|"
                          "com.lightstreamer.demo.ios.stocklistdemo|S|"
                          "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                          "24cff14a9906f1|S|0849781a0afe0311f58bbfee1fcde031b"
                          "fc56635c89c566dda3c6708fd893549")
        self.assert_reply("3700000147c9bc4c74|MDC|V")

        device = MpnDeviceInfo(platform_type="",
                               application_id=("com.lightstreamer.demo.ios."
                                               "stocklistdemo"),
                               device_token="f780e9d8ffc86a5ec9a329e7745aa8fb"
                                            "3a1ecce77c""09e202ec24cff14a9906""f1")
        expected_dict = {"user": "user1",
                         "sessionId": "S8f3da29cfc463220T5454537",
                         "mpnDeviceInfo": device,
                         "newDeviceToken": "0849781a0afe0311f58bbfee1fcde031b"
                                           "fc56635c89c566dda3c6708fd893549"}
        self.assertDictEqual(expected_dict, self.collector)

    def test_notify_mpn_device_token_change_with_notification_exception(self):
        self.do_init_and_skip()
        self.send_request("3700000147c9bc4c74|MDC|S|#"
                          "|S|S8f3da29cfc463220T5454537|P|A|S|"
                          "com.lightstreamer.demo.ios.stocklistdemo|S|"
                          "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                          "24cff14a9906f1|S|0849781a0afe0311f58bbfee1fcde031b"
                          "fc56635c89c566dda3c6708fd893549")
        self.assert_reply("3700000147c9bc4c74|MDC|EN|NotificationError")

    def test_notify_mpn_device_token_change_with_credits_exception(self):
        self.do_init_and_skip()
        self.send_request("3700000147c9bc4c74|MDC|S|$"
                          "|S|S8f3da29cfc463220T5454537|P|A|S|"
                          "com.lightstreamer.demo.ios.stocklistdemo|S|"
                          "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                          "24cff14a9906f1|S|0849781a0afe0311f58bbfee1fcde031b"
                          "fc56635c89c566dda3c6708fd893549")
        self.assert_reply("3700000147c9bc4c74|MDC|EC|CreditsError|10|"
                          "clientErrorMsg")

    def test_notify_mpn_device_token_change_with_generic_exception(self):
        self.do_init_and_skip()
        self.send_request("3700000147c9bc4c74|MDC|S|user2"
                          "|S|S8f3da29cfc463220T5454537|P|A|S|"
                          "com.lightstreamer.demo.ios.stocklistdemo|S|"
                          "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                          "24cff14a9906f1|S|0849781a0afe0311f58bbfee1fcde031b"
                          "fc56635c89c566dda3c6708fd893549")
        self.assert_reply("3700000147c9bc4c74|MDC|E|Exception")

    def test_malformed_notify_mpn_device_token_change(self):
        self.do_init_and_skip()
        self.send_request("3700000147c9bc4c74|MDC|S|user1"
                          "|S|S8f3da29cfc463220T5454537|P|B1|S|"
                          "com.lightstreamer.demo.ios.stocklistdemo|S|"
                          "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                          "24cff14a9906f1|S|"
                          "0849781a0afe0311f58bbfee1fcde031bfc56635c89c566dda"
                          "3c6708fd893549")
        self.assert_caught_exception("Unknown platform type 'B1' while parsing"
                                     " MDC request")


if __name__ == "__main__":
    # import syssys.argv = ['', 'DataProviderTest.testName']
    unittest.main()
