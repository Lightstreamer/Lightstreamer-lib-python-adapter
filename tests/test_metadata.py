'''
Created on 20 gen 2016

@author: Gianluca
'''
import unittest
import logging

from common import RemoteAdapterBase

from lightstreamer.adapter.server import MetadataProviderServer, \
    ExceptionHandler
from lightstreamer.interfaces.metadata import (MetadataProvider,
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
                                               MpnApnsSubscriptionInfo,
                                               MpnGcmSubscriptionInfo)
import queue

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


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

        return super(MetadataProviderTestClass, self).get_items(user,
                                                                session_id,
                                                                group)

    def get_allowed_max_bandwidth(self, user):
        return 12.3

    def wants_tables_notification(self, user):
        return True

    def get_distinct_snapshot_length(self, item):
        if item == "item_1":
            return 10

        if item == "item_2":
            return 20

        if item == "item_4":
            raise CreditsError(4, "CreditsError", "clientErrorMsg")

        return 0

    def get_min_source_frequency(self, item):
        if item == "item_1":
            return 4.5

        if item == "item_2":
            return 7.3

        return 0

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

        return 0

    def get_allowed_buffer_size(self, user, item):
        if item == "item_1":
            return 30

        if item == "item_2":
            return 40

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
                               'password': password,
                               'httpHeaders': http_headers})

        if user == "user1":
            raise CreditsError(10, "CreditsError", "User not allowed")

        if user == "user2":
            raise RuntimeError("Error while notifying")

        if user is None:
            raise AccessError("A NULL user is not allowed")

        if not user.strip():
            raise AccessError("An empty user is not allowed")

    def notify_user_with_client_principal(self, user, password, http_headers,
                                          client_principal=None):
        self.collector.update({'user': user,
                               'password': password,
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

    def notify_mpn_device_access(self, user, device):
        if user is None:
            raise NotificationError("NotificationError")

        if not user:
            raise CreditsError(10, "CreditsError", "clientErrorMsg")

        if user == "user2":
            raise RuntimeError("Exception")

        self.collector.update({"user": user,
                               "mpnDeviceInfo": device})

    def notify_mpn_subscription_activation(self, user, sessionID, table,
                                           mpn_subscription):
        if user is None:
            raise NotificationError("NotificationError")

        if not user:
            raise CreditsError(10, "CreditsError", "clientErrorMsg")

        if user == "user2":
            raise RuntimeError("Exception")

        self.collector.update({"user": user,
                               "sessionId": sessionID,
                               "table": table,
                               "mpn_subscription": mpn_subscription})

    def notify_mpn_device_token_change(self, user, device, new_device_token):
        if user is None:
            raise NotificationError("NotificationError")

        if not user:
            raise CreditsError(10, "CreditsError", "clientErrorMsg")

        if user == "user2":
            raise RuntimeError("Exception")
        self.collector.update({"user": user,
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
        self.assertEqual("nasdaq100_AA_AL", tb1.id)
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
        device_info_1 = MpnDeviceInfo(MpnPlatformType.APNS, "applicationId",
                                      "deviceToken")

        self.assertEqual(MpnPlatformType.APNS, device_info_1.mpn_platform_type)
        self.assertEqual("applicationId", device_info_1.application_id)
        self.assertEqual("deviceToken", device_info_1.device_token)

    def test_equality(self):
        device_info_1 = MpnDeviceInfo(MpnPlatformType.MPNS, "applicationId",
                                      "deviceToken")
        device_info_2 = MpnDeviceInfo(MpnPlatformType.MPNS, "applicationId",
                                      "deviceToken")
        self.assertEqual(device_info_1, device_info_2)

    def test_not_equality(self):
        device_info_1 = MpnDeviceInfo(MpnPlatformType.APNS, "applicationId",
                                      "deviceToken")
        device_info_2 = MpnDeviceInfo(MpnPlatformType.MPNS, "applicationId",
                                      "deviceToken")
        self.assertNotEqual(device_info_1, device_info_2)

        device_info_2 = MpnDeviceInfo(MpnPlatformType.APNS, "applicationId2",
                                      "deviceToken")
        self.assertNotEqual(device_info_1, device_info_2)

        device_info_2 = MpnDeviceInfo(MpnPlatformType.APNS, "applicationId",
                                      "deviceToken2")
        self.assertNotEqual(device_info_1, device_info_2)


class MyExceptionHandler(ExceptionHandler):

    def __init__(self):
        self._caught_exception_queue = queue.Queue()

    def handle_io_exception(self, ioexception):
        print("Got IO Exception {}".format(ioexception))
        return False

    def handle_exception(self, exception):
        print("Caught exception: {}".format(str(exception.__cause__)))
        self._caught_exception_queue.put(str(exception))
        self._caught_exception_queue.task_done()
        return False

    def get(self):
        return self._caught_exception_queue.get()

    def join(self):
        self._caught_exception_queue.join()


class MetadataProviderTest(RemoteAdapterBase):

    def on_setup(self):
        self.collector = {}

        # Configuring and starting MetadataProviderServer
        self.adapter = MetadataProviderTestClass(self.collector)
        server = MetadataProviderServer(self.adapter,
                                        self.get_req_reply_address(),
                                        keep_alive=0)
        self.exception_handler = MyExceptionHandler()
        server.set_exception_handler(self.exception_handler)
        return server

    def on_teardown(self, server):
        self.exception_handler.join()
        log.info("MetadataProviderTest completed")

    def assert_caught_exception(self, msg):
        self.assertEqual(msg, self.exception_handler.get())

    def do_init_and_skip(self):
        self.send_request("10000010c3e4d0462|MPI", True)

    def test_init(self):
        self.send_request(("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO|S|"
                           "proxy.instance_id|S|hewbc3ikbbctyui"))
        self.assert_reply('10000010c3e4d0462|MPI|V')
        self.assertEqual({"adapters_conf.id": "DEMO", "proxy.instance_id":
                          "hewbc3ikbbctyui"}, self.collector['params'])
        self.assertIsNone(self.adapter.config_file)

    def test_init_with_adapter_config(self):
        self.remote_adapter.adapter_config = "config.file"
        self.do_init_and_skip()
        self.assertEqual("config.file", self.adapter.config_file)

    def test_init_with_local_params(self):
        self.remote_adapter.adapter_params = {"par1": "val1", "par2": "val2"}
        self.send_request("10000010c3e4d0462|MPI")

        self.assert_reply("10000010c3e4d0462|MPI|V")
        self.assertDictEqual({"par1": "val1",
                              "par2": "val2"},
                             self.collector['params'])

    def test_init_with_remote_params(self):
        self.send_request(("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO|S|"
                           "proxy.instance_id|S|hewbc3ikbbctyui"))

        self.assert_reply("10000010c3e4d0462|MPI|V")
        self.assertDictEqual({"adapters_conf.id": "DEMO",
                              "proxy.instance_id": "hewbc3ikbbctyui"},
                             self.collector['params'])

    def test_init_with_local_and_remote_params(self):
        self.remote_adapter.adapter_params = {"proxy.instance_id":
                                              "my_local_meta_provider"}
        self.send_request(("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO|S|"
                           "proxy.instance_id|S|hewbc3ikbbctyui"))
        self.assert_reply("10000010c3e4d0462|MPI|V")
        self.assertDictEqual({"adapters_conf.id": "DEMO",
                              "proxy.instance_id":
                              "my_local_meta_provider"},
                             self.collector['params'])

    def test_init_with_metadata_provider_exception(self):
        self.send_request(("10000010c3e4d0462|MPI|S|proxy.instance_id|S|"
                           "hewbc3ikbbctyui"))
        self.assert_reply("10000010c3e4d0462|MPI|EM|The+ID+must+be+supplied")

    def test_init_with_generic_exception(self):
        self.send_request("10000010c3e4d0462|MPI|S|adapters_conf.id|S|DEMO")
        self.assert_reply("10000010c3e4d0462|MPI|E|Exception")

    def test_malformed_init_for_unkown_token_type(self):
        self.send_request('10000010c3e4d0462|MPI|S|adapters_conf.id|S1|DEMO')
        self.assert_caught_exception(("Unknown type 'S1' found while parsing a"
                                      " MPI request"))

    def test_malformed_init_for_invalid_number_of_tokens(self):
        self.send_request('10000010c3e4d0462|MPI|S|')
        self.assert_caught_exception(("Invalid number of tokens while parsing "
                                      "a MPI request"))

    def test_malformed_init_for_invalid_number_of_tokens2(self):
        self.send_request('10000010c3e4d0462|MPI|S||')
        self.assert_caught_exception(("Invalid number of tokens while parsing "
                                      "a MPI request"))

    def test_malformed_init_for_invalid_number_of_tokens3(self):
        self.send_request('10000010c3e4d0462|MPI|S|  |')
        self.assert_caught_exception(("Invalid number of tokens while parsing "
                                      "a MPI request"))

    def test_malformed_init_for_invalid_number_of_tokens4(self):
        self.send_request('10000010c3e4d0462|MPI|S|id|S')
        self.assert_caught_exception(("Invalid number of tokens while parsing "
                                      "a MPI request"))

    def test_init_init(self):
        # Test error when more than one MPI request is issued
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|MPI")
        self.assert_caught_exception("Unexpected late MPI request")

    def test_init_miss(self):
        # Test error when the very first request is not a MPI request
        self.send_request(("10000010c3e4d0462|NUS|S|userX|S|password|S|host|S|"
                           "www.mycompany.com"))

        self.assert_caught_exception(("Unexpected request NUS while waiting "
                                      "for a MPI request"))

    def test_notify_user(self):
        self.do_init_and_skip()
        self.send_request(("10000010c3e4d0462|NUS|S|userX|S|password|S|host|S|"
                           "www.mycompany.com"))

        self.assert_reply("10000010c3e4d0462|NUS|D|12.3|B|1")
        self.assertDictEqual({"user": "userX",
                              "password": "password",
                              "httpHeaders": {"host": "www.mycompany.com"}},
                             self.collector)

    def test_notify_user_with_no_http_headers(self):
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|NUS|S|userX|S|password")
        self.assert_reply("10000010c3e4d0462|NUS|D|12.3|B|1")
        self.assertDictEqual({"user": "userX",
                              "password": "password",
                              "httpHeaders": {}},
                             self.collector)

    def test_notify_user_with_credits_exception(self):
        # Testing CreditsError
        self.do_init_and_skip()
        self.send_request(("10000010c3e4d0462|NUS|S|user1|S|password|S|host|S|"
                           "www.mycompany.com"))
        self.assert_reply(("10000010c3e4d0462|NUS|EC|CreditsError|10|User+not+"
                           "allowed"))

    def test_notify_user_with_access_exception(self):
        # Testing AccessError
        self.do_init_and_skip()
        self.send_request(("10000010c3e4d0462|NUS|S|$|S|password|S|host|S|"
                           "www.mycompany.com"))
        self.assert_reply(("10000010c3e4d0462|NUS|EA|An+empty+user+is+not+"
                           "allowed"))

    def test_notify_user_with_access_exception2(self):
        # Testing AccessError with null value for user
        self.do_init_and_skip()
        self.send_request(("10000010c3e4d0462|NUS|S|#|S|#|S|host|S|"
                           "www.mycompany.com|"))
        self.assert_reply(("10000010c3e4d0462|NUS|EA|A+NULL+user+is+not+"
                           "allowed"))

    def test_notify_user_with_generic_exception(self):
        # Testing other errors
        self.do_init_and_skip()
        self.send_request(("10000010c3e4d0462|NUS|S|user2|S|password|S|host|S|"
                           "www.mycompany.com"))
        self.assert_reply("10000010c3e4d0462|NUS|E|Error+while+notifying")

    def test_malformed_notify_user(self):
        self.do_init_and_skip()
        # user and password are missing.
        self.send_request("10000010c3e4d0462|NUS")
        self.assert_caught_exception(("Token not found while parsing a NUS "
                                      "request"))

        # user and password are missing.
        self.send_request("10000010c3e4d0462|NUS|S|")
        self.assert_caught_exception(("Token not found while parsing a NUS "
                                     "request"))

        # Wrong token type for user.
        self.send_request("10000010c3e4d0462|NUS|S1|user")
        self.assert_caught_exception(("Unknown type 'S1' found while parsing a"
                                      " NUS request"))

        # password is missing
        self.send_request("10000010c3e4d0462|NUS|S|user")
        self.assert_caught_exception(("Token not found while parsing a NUS "
                                      "request"))

        # password is missing
        self.send_request("10000010c3e4d0462|NUS|S|user|S")
        self.assert_caught_exception(("Token not found while parsing a NUS "
                                      "request"))

        # Wrong token type for password.
        self.send_request("10000010c3e4d0462|NUS|S|user|S2")
        self.assert_caught_exception(("Unknown type 'S2' found while parsing a"
                                      " NUS request"))

    def test_notify_user_auth(self):
        self.do_init_and_skip()
        self.send_request(("10000010c3e4d0462|NUA|S|userXA|S|password|S|"
                           "<CLIENT_PRINCIPAL>|S|<HEADER_1>|S|<HEADER_VALUE1>|"
                           "S|<HEADER_2>|S|<HEADER_VALUE2>"))

        self.assert_reply("10000010c3e4d0462|NUA|D|12.3|B|1")
        self.assertDictEqual({"user": "userXA",
                              "password": "password",
                              "clientPrincipal": '<CLIENT_PRINCIPAL>',
                              "httpHeaders": {"<HEADER_1>": "<HEADER_VALUE1>",
                                              "<HEADER_2>": "<HEADER_VALUE2>"}
                              },
                             self.collector)

    def test_notify_user_with_auth_credits_exception(self):
        # Testing CreditsError
        self.do_init_and_skip()
        self.send_request(("10000010c3e4d0462|NUA|S|user1|S|password|S|"
                           "<CLIENT_PRINCIPAL>|S|<HEADER_1>|S|<HEADER_VALUE1>|"
                           "S|<HEADER_2>|S|<HEADER_VALUE2>"))
        self.assert_reply(("10000010c3e4d0462|NUA|EC|CreditsError|10|User+not+"
                           "allowed"))

    def test_notify_user_with_auth_access_exception(self):
        # Testing AccessError
        self.do_init_and_skip()
        self.send_request(("10000010c3e4d0462|NUA|S|$|S|password|S|"
                           "<CLIENT_PRINCIPAL>|S|<HEADER_1>|S|<HEADER_VALUE1>|"
                           "S|<HEADER_2>|S|<HEADER_VALUE2>"))
        self.assert_reply(("10000010c3e4d0462|NUA|EA|An+empty+user+is+not+"
                           "allowed"))

    def test_notify_user_auth_with_generic_exception(self):
        # Testing other errors
        self.do_init_and_skip()
        self.send_request(("10000010c3e4d0462|NUA|S|user2|S|password|S|"
                           "<CLIENT_PRINCIPAL>|S|<HEADER_1>|S|<HEADER_VALUE1>|"
                           "S|<HEADER_2>|S|<HEADER_VALUE2>"))

        self.assert_reply("10000010c3e4d0462|NUA|E|Error+while+notifying")

    def test_malformed_notify_user_auth(self):
        self.do_init_and_skip()
        self.send_request("10000010c3e4d0462|NUA|S|")
        self.assert_caught_exception(("Token not found while parsing a NUA "
                                      "request"))

        self.send_request("10000010c3e4d0462|NUA|S1|user")
        self.assert_caught_exception(("Unknown type 'S1' found while parsing a"
                                      " NUA request"))

    def test_notify_new_session(self):
        self.do_init_and_skip()
        self.send_request(("10000020b3e6d0462|NNS|S|<user name>|S|"
                           "<session ID>|S|<context prop name 1>|S|"
                           "<context prop 1 value>|S|<context prop name N>|S|"
                           "<context prop N value>"))

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
        self.send_request(("10000020b3e6d0462|NNS|S|user1|S|<session ID>|S|"
                           "<context prop name 1>|S|<context prop 1 value>|S|"
                           "<context prop name N>|S|<context prop N value>"))
        self.assert_reply(("10000020b3e6d0462|NNS|EC|CreditsError|9|User+not+"
                           "allowed"))

    def test_notify_new_session_with_notification_exception(self):
        # Testing NotificationError
        self.do_init_and_skip()
        self.send_request(("10000020b3e6d0462|NNS|S|user2|S|<session ID>|S|"
                           "<context prop name 1>|S|<context prop 1 value>|S|"
                           "<context prop name N>|S|<context prop N value>"))
        self.assert_reply("10000020b3e6d0462|NNS|EN|NotificationError")

    def test_notify_new_session_with_conflicting_session_exception(self):
        # Testing ConflictingSessionError
        self.do_init_and_skip()
        self.send_request(("10000020b3e6d0462|NNS|S|user3|S|<session ID>|S|"
                           "<context prop name 1>|S|<context prop 1 value>|S|"
                           "<context prop name N>|S|<context prop N value>"))
        self.assert_reply(("10000020b3e6d0462|NNS|EX|ConflictingSessionError|"
                           "11|clientErrorMsg|conflictingSessionID"))

    def test_notify_new_session_with_generic_exception(self):
        # Testing generic Error
        self.do_init_and_skip()
        self.send_request(("10000020b3e6d0462|NNS|S|user4|S|<session ID>|S|"
                           "<context prop name 1>|S|<context prop 1 value>|S|"
                           "<context prop name N>|S|<context prop N value>"))
        self.assert_reply("10000020b3e6d0462|NNS|E|Error+while+notifying")

    def test_malformed_notify_new_session(self):
        self.do_init_and_skip()
        self.send_request("10000020b3e6d0462|NNS|S|")
        self.assert_caught_exception(("Token not found while parsing a NNS "
                                      "request"))

        self.send_request("10000020b3e6d0462|NNS|S1|user4")
        self.assert_caught_exception(("Unknown type 'S1' found while parsing a"
                                      " NNS request"))

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
        self.assert_caught_exception(("Token not found while parsing a NSC "
                                      "request"))

        self.send_request("20000010c3e4d0462|NSC|S1|user4")
        self.assert_caught_exception(("Unknown type 'S1' found while parsing a"
                                      " NSC request"))

    def test_get_items(self):
        self.do_init_and_skip()
        self.send_request(("50000010c3e4d0462|GIS|S|user1|S|nasdaq100_AA_AL+"
                           "abc|S|S8f3da29cfc463220T5454537"))
        self.assert_reply("50000010c3e4d0462|GIS|S|nasdaq100_AA_AL|S|abc")
        self.assertDictEqual({"user": "user1",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL abc"},
                             self.collector)

    def test_get_items_with_items_exception(self):
        # Testing ItemsError Error provoked by an empty user ()
        self.do_init_and_skip()
        self.send_request(("50000010c3e4d0462|GIS|S|$|S|nasdaq100_AA_AL+abc|S|"
                           "S8f3da29cfc463220T5454537"))
        self.assert_reply("50000010c3e4d0462|GIS|EI|Empty+User")
        self.assertDictEqual({"user": "",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL abc"},
                             self.collector)

    def test_get_items_with_generic_exception(self):
        # Testing generic Error provoked by a null user (#)
        self.do_init_and_skip()
        self.send_request(("50000010c3e4d0462|GIS|S|#|S|nasdaq100_AA_AL+abc|S|"
                           "S8f3da29cfc463220T5454537"))

        self.assert_reply("50000010c3e4d0462|GIS|E|NULL+User")
        self.assertDictEqual({"user": None,
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL abc"},
                             self.collector)

    def test_malformed_get_items(self):
        self.do_init_and_skip()
        self.send_request("50000010c3e4d0462|GIS|S|")
        self.assert_caught_exception(("Token not found while parsing a GIS "
                                      "request"))

        self.send_request("50000010c3e4d0462|GIS|S1|nasdaq100_AA_AL")
        self.assert_caught_exception(("Unknown type 'S1' found while parsing a"
                                      " GIS request"))

    def test_get_schema(self):
        self.do_init_and_skip()
        self.send_request(("70000010c3e4d0462|GSC|S|user1|S|nasdaq100_AA_AL+"
                           "abc|S|short1+short2|S|S8f3da29cfc463220T5454537"))
        self.assert_reply("70000010c3e4d0462|GSC|S|short1|S|short2")
        self.assertDictEqual({"user": "user1",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL abc",
                              "schema": "short1 short2"},
                             self.collector)

    def test_get_schema_with_items_exception(self):
        # Testing ItemsError provoked by null user (#)
        self.do_init_and_skip()
        self.send_request(("70000010c3e4d0462|GSC|S|#|S|nasdaq100_AA_AL+abc|S|"
                           "short1+short2|S|S8f3da29cfc463220T5454537"))
        self.assert_reply("70000010c3e4d0462|GSC|EI|NULL+User")
        self.assertDictEqual({"user": None,
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL abc",
                              "schema": "short1 short2"},
                             self.collector)

    def test_get_schema_with_schema_exception(self):
        self.do_init_and_skip()
        self.send_request(("70000010c3e4d0462|GSC|S|user1|S|nasdaq100_AA_AL|S|"
                           "shortA|S|S8f3da29cfc463220T5454537"))
        self.assert_reply("70000010c3e4d0462|GSC|ES|SchemaError")
        self.assertDictEqual({"user": "user1",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL",
                              "schema": "shortA"},
                             self.collector)

    def test_get_schema_with_generic_exception(self):
        # Testing ItemsEerror provoked by an empty user
        self.do_init_and_skip()
        self.send_request(("70000010c3e4d0462|GSC|S|$|S|nasdaq100_AA_AL+abc|S|"
                           "short1+short2|S|S8f3da29cfc463220T5454537"))
        self.assert_reply("70000010c3e4d0462|GSC|E|Empty+User")
        self.assertDictEqual({"user": "",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "group": "nasdaq100_AA_AL abc",
                              "schema": "short1 short2"},
                             self.collector)

    def test_malformed_get_schema(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GSC|S|")
        self.assert_caught_exception(("Token not found while parsing a GSC "
                                     "request"))

        self.send_request("70000010c3e4d0462|GSC|S1|nasdaq100_AA_AL")
        self.assert_caught_exception(("Unknown type 'S1' found while parsing a"
                                      " GSC request"))

    def test_get_item_data(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GIT|S|item_1|S|item_2|S|item_3")
        self.assert_reply(("70000010c3e4d0462|GIT|I|10|D|4.5|M|MD|I|20|D|7.3"
                           "|M|RMC|I|0|D|0.0|M|$"))

    def test_get_item_data_with_no_specified_item(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GIT")
        self.assert_reply("70000010c3e4d0462|GIT")

    def test_get_item_data_with_exception(self):
        # Testing generic Error provoked by item_4
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GIT|S|item_1|S|item_2|S|item_4")
        self.assert_reply("70000010c3e4d0462|GIT|E|CreditsError")

    def test_malformed_get_item_data(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GIT|S|")
        self.assert_caught_exception(("Token not found while parsing a GIT "
                                      "request"))

        self.send_request("70000010c3e4d0462|GIT|S1|item")
        self.assert_caught_exception(("Unknown type 'S1' found while parsing a"
                                      " GIT request"))

    def test_get_user_item_data(self):
        self.do_init_and_skip()
        self.send_request(("70000010c3e4d0462|GUI|S|user1|S|item_1|S|item_2|S|"
                           "item_3"))
        self.assert_reply(("70000010c3e4d0462|GUI|I|30|D|170.5|M|RMDC|I|40|D|"
                           "27.3|M|RM|I|0|D|0.0|M|$"))

    def test_get_user_item_data_with_no_specified_items(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GUI|S|user1")
        self.assert_reply("70000010c3e4d0462|GUI")

    def test_get_user_item_data_with_exception(self):
        # Testing generic Error provoked by user2
        self.do_init_and_skip()
        self.send_request(("70000010c3e4d0462|GUI|S|user2|S|item_1|S|item_2|S|"
                           "item_4"))
        self.assert_reply("70000010c3e4d0462|GUI|E|No+user+allowed")

    def test_malformed_get_user_item_data(self):
        self.do_init_and_skip()
        self.send_request("70000010c3e4d0462|GUI")
        self.assert_caught_exception(("Token not found while parsing a GUI "
                                      "request"))

        self.send_request("70000010c3e4d0462|GUI|S|")
        self.assert_caught_exception(("Token not found while parsing a GUI "
                                      "request"))

        self.send_request("70000010c3e4d0462|GUI|S1|user2")
        self.assert_caught_exception(("Unknown type 'S1' found while parsing a"
                                      " GUI request"))

    def test_notify_user_message(self):
        self.do_init_and_skip()
        self.send_request(("d0000010c3e4d0462|NUM|S|user1|S|"
                           "S8f3da29cfc463220T5454537|S|stop+logging"))
        self.assert_reply("d0000010c3e4d0462|NUM|V")
        self.assertDictEqual({"user": "user1",
                              "sessionId": "S8f3da29cfc463220T5454537",
                              "message": "stop logging"},
                             self.collector)

    def test_notify_user_message_with_notification_exception(self):
        self.do_init_and_skip()
        self.send_request(("d0000010c3e4d0462|NUM|S|user2|S|"
                           "S8f3da29cfc463220T5454537|S|stop+logging"))
        self.assert_reply("d0000010c3e4d0462|NUM|EN|NotificationError")

    def test_notify_user_message_with_credits_exception(self):
        self.do_init_and_skip()
        self.send_request(("d0000010c3e4d0462|NUM|S|user3|S|"
                           "S8f3da29cfc463220T5454537|S|stop+logging"))
        self.assert_reply(("d0000010c3e4d0462|NUM|EC|CreditsError|4|"
                           "clientErrorMsg"))

    def test_notify_user_message_with_generic_exception(self):
        self.do_init_and_skip()
        self.send_request(("d0000010c3e4d0462|NUM|S|user4|S|"
                           "S8f3da29cfc463220T5454537|S|stop+logging"))
        self.assert_reply("d0000010c3e4d0462|NUM|E|Exception")

    def test_malformed_notify_user_message(self):
        self.do_init_and_skip()
        self.send_request("d0000010c3e4d0462|NUM|S|")
        self.assert_caught_exception(("Token not found while parsing a NUM "
                                      "request"))

        self.send_request("d0000010c3e4d0462|NUM|S1|user4")
        self.assert_caught_exception(("Unknown type 'S1' found while parsing a"
                                      " NUM request"))

    def test_notify_new_tables(self):
        self.do_init_and_skip()
        self.send_request(("f0000010c3e4d0462|NNT|S|#|S|"
                           "S8f3da29cfc463220T5454537|I|1|M|M|S|"
                           "nasdaq100_AA_AL|S|short|I|1|I|5|S|#|I|2|M|D|S|"
                           "nasdaq100_AA_AL|S|medium|I|4|I|3|S|selector"))

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
                                                       selector='selector')]
                              },
                             self.collector)

    def test_notify_new_tables_with_null_mode(self):
        self.do_init_and_skip()
        self.send_request(("f0000010c3e4d0462|NNT|S|#|S|"
                           "S8f3da29cfc463220T5454537|I|1|M|#|S|"
                           "nasdaq100_AA_AL|S|short|I|1|I|5|S|#|I|2|M|D|S|"
                           "nasdaq100_AA_AL|S|medium|I|4|I|3|S|selector"))
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
        self.send_request(("f0000010c3e4d0462|NNT|S|#|S|"
                           "S8f3da29cfc463220T5454537|I|1|M|$|S|"
                           "nasdaq100_AA_AL|S|short|I|1|I|5|S|#|I|2|M|D|S|"
                           "nasdaq100_AA_AL|S|medium|I|4|I|3|S|selector"))
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
        self.send_request(("f0000010c3e4d0462|NNT|S|user1|S|"
                           "S8f3da29cfc463220T5454537|I|1|M|M|S|"
                           "nasdaq100_AA_AL|S|short|I|1|I|5|S|#"))
        self.assert_reply(("f0000010c3e4d0462|NNT|EC|CreditsError|10|"
                           "clientErrorMsg"))

    def test_notify_new_tables_with_notification_exception(self):
        self.do_init_and_skip()
        self.send_request(("f0000010c3e4d0462|NNT|S|user2|S|"
                           "S8f3da29cfc463220T5454537|I|1|M|M|S|"
                           "nasdaq100_AA_AL|S|short|I|1|I|5|S|#"))
        self.assert_reply("f0000010c3e4d0462|NNT|EN|NotificationError")

    def test_notify_new_tables_with_exception(self):
        self.do_init_and_skip()
        self.send_request(("f0000010c3e4d0462|NNT|S|user3|S|"
                           "S8f3da29cfc463220T5454537|I|1|M|M|S|"
                           "nasdaq100_AA_AL|S|short|I|1|I|5|S|#"))
        self.assert_reply("f0000010c3e4d0462|NNT|E|Exception")

    def test_malformed_notify_new_tables(self):
        self.do_init_and_skip()
        self.send_request("f0000010c3e4d0462|NNT|S|#|S")
        self.assert_caught_exception(("Token not found while parsing a NNT "
                                      "request"))

        self.send_request("f0000010c3e4d0462|NNT|S1|#")
        self.assert_caught_exception(("Unknown type 'S1' found while parsing a"
                                      " NNT request"))

    def test_notify_tables_close(self):
        self.do_init_and_skip()
        self.send_request(("f0000010c3e4d0462|NTC|S|S8f3da29cfc463220T5454537|"
                           "I|1|M|M|S|nasdaq100_AA_AL|S|short|I|1|I|5|S|#"))
        self.assert_reply("f0000010c3e4d0462|NTC|V")
        self.assertDictEqual({"sessionId": "S8f3da29cfc463220T5454537",
                              "tableInfos": [TableInfo(win_index=1,
                                                       mode=Mode.MERGE,
                                                       group="nasdaq100_AA_AL",
                                                       schema="short",
                                                       first_idx=1,
                                                       last_idx=5)]
                              },
                             self.collector)

    def test_notify_tables_close_with_no_table_info(self):
        self.do_init_and_skip()
        self.send_request("f0000010c3e4d0462|NTC|S|S8f3da29cfc463220T5454537")
        self.assert_reply("f0000010c3e4d0462|NTC|V")
        self.assertDictEqual({"sessionId": "S8f3da29cfc463220T5454537",
                              "tableInfos": []},
                             self.collector)

    def test_notify_tables_close_with_notification_exception(self):
        self.do_init_and_skip()
        self.send_request(("f0000010c3e4d0462|NTC|S|S8f3da29cfc463220T5454539|"
                           "I|1|M|M|S|nasdaq100_AA_AL|S|short|I|1|I|5|S|#"))
        self.assert_reply("f0000010c3e4d0462|NTC|EN|NotificationError")

    def test_notify_tables_close_with_generic_exception(self):
        self.do_init_and_skip()
        self.send_request(("f0000010c3e4d0462|NTC|S|S8f3da29cfc463220T5454540|"
                           "I|1|M|M|S|nasdaq100_AA_AL|S|short|I|1|I|5|S|#"))
        self.assert_reply("f0000010c3e4d0462|NTC|E|Exception")

    def test_malformed_notify_tables_close(self):
        self.do_init_and_skip()
        # A "S" token type is expected.
        self.send_request("f0000010c3e4d0462|NTC|S1|#")
        self.assert_caught_exception(("Unknown type 'S1' found while parsing a"
                                      " NTC request"))

        # A "I" token type is expected.
        self.send_request("f0000010c3e4d0462|NTC|S|#|S1")
        self.assert_caught_exception(("Unknown type 'S1' found while parsing a"
                                      " NTC request"))

    def test_notify_device_access(self):
        self.do_init_and_skip()
        self.send_request(("b00000147c9bc4c74|MDA|S|user1|P|A|S|"
                           "com.lightstreamer.demo.ios.stocklistdemo|S|"
                           "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                           "24cff14a9906f1"))
        self.assert_reply("b00000147c9bc4c74|MDA|V")
        self.assertDictEqual({"user": "user1",
                              "mpnDeviceInfo":
                              MpnDeviceInfo(platform_type=MpnPlatformType.APNS,
                                            application_id=("com.lightstreamer"
                                                            ".demo.ios."
                                                            "stocklistdemo"),
                                            device_token=("f780e9d8ffc86a5ec9a"
                                                          "329e7745aa8fb3a1ecc"
                                                          "e77c09e202ec24cff14"
                                                          "a9906f1"))},
                             self.collector)

    def test_notify_device_access_with_notification_exception(self):
        self.do_init_and_skip()
        self.send_request(("b00000147c9bc4c74|MDA|S|#|P|A|S|"
                           "com.lightstreamer.demo.ios.stocklistdemo|S|"
                           "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                           "24cff14a9906f1"))
        self.assert_reply("b00000147c9bc4c74|MDA|EN|NotificationError")

    def test_notify_device_access_with_credits_exception(self):
        self.do_init_and_skip()
        self.send_request(("b00000147c9bc4c74|MDA|S|$|P|A|S|"
                           "com.lightstreamer.demo.ios.stocklistdemo|S|"
                           "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                           "24cff14a9906f1"))
        self.assert_reply(("b00000147c9bc4c74|MDA|EC|CreditsError|10|"
                           "clientErrorMsg"))

    def test_malformed_notify_device_access(self):
        self.do_init_and_skip()
        self.send_request("b00000147c9bc4c74|MDA|S|#|P")
        self.assert_caught_exception(("Token not found while parsing a MDA "
                                      "request"))

        self.send_request("b00000147c9bc4c74|MDA|S1|#|P")
        self.assert_caught_exception(("Unknown type 'S1' found while parsing a"
                                      " MDA request"))

        self.send_request("b00000147c9bc4c74|MDA|S|#|P|Q")
        self.assert_caught_exception(("Unknown platform type 'Q' while parsing"
                                      " a MDA request"))

    def test_notify_device_access_with_generic_exception(self):
        self.do_init_and_skip()
        self.send_request(("b00000147c9bc4c74|MDA|S|user2|P|A|S|"
                           "com.lightstreamer.demo.ios.stocklistdemo|S|"
                           "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                           "24cff14a9906f1"))
        self.assert_reply("b00000147c9bc4c74|MDA|E|Exception")

    def test_notify_mpn_subscription_activation_APN(self):
        self.do_init_and_skip()
        request = ("c00000147c9bc4c74|MSA|S|user1|S|Sc4a1769b6bb83a4aT2852044|"
                   # I|<win. index>|M|<pub. mode>|S|<item group>|S|
                   # <field schema>|
                   "I|1|M|M|S|item4+item19|S|stock_name+last_price+time|"
                   # I|<first item idx.>|I|<last item idx.>|
                   "I|1|I|2|"
                   # PA|<num. of arguments>|<num. of custom test_data>|
                   "PA|2|4|"
                   # P|A|S|<application ID>|S|<device token>|S|<trigger>|
                   "P|A|S|com.lightstreamer.demo.ios.stocklistdemo|S|"
                   "f74d8ffc5ee7cb31749a329a8f9202867c0a9906e80ee7f9eeccca1f24"
                   "c5aaf1|S|"
                   "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+1000.0|"
                   # S|<sound name>|S|<icon badge>|S|<loc. action key>|
                   "S|Default|S|AUTO|S|MARKET+VIEW|"
                   # S|<launch image name>|S|<format>|S|<loc. format key>|
                   "S|#|S|#|S|MARKET+UPDATE|"
                   # S|<arg. 1>|S|<arg. 2>|...|S|<arg. N>|
                   "S|%24%7Bstock_name%7D|S|%24%7Blast_price%7D|"
                   # S|<custom test_data key 1>|S|<custom value key 1>|...|
                   "S|customKey1|S|123456|"
                   "S|customKey2|S|7890|"
                   "S|customKey3|S|#|"
                   "S|customKey4|S|$")

        self.send_request(request)
        self.assert_reply("c00000147c9bc4c74|MSA|V")

        device = MpnDeviceInfo(
                     platform_type=MpnPlatformType.APNS,
                     application_id="com.lightstreamer.demo.ios.stocklistdemo",
                     device_token=("f74d8ffc5ee7cb31749a329a8f9202867c0a9906e8"
                                   "0ee7f9eeccca1f24c5aaf1"))

        subscription = MpnApnsSubscriptionInfo(
                          device=device,
                          trigger="Double.parseDouble(${last_price}) > 1000.0",
                          sound="Default",
                          badge="AUTO",
                          launch_image=None,
                          format=None,
                          localized_action_key="MARKET VIEW",
                          localized_format_key="MARKET UPDATE",
                          arguments=['${stock_name}', '${last_price}'],
                          custom_data={'customKey1': '123456',
                                       'customKey2': '7890',
                                       'customKey3': None,
                                       'customKey4': ''})

        table_info = TableInfo(win_index=1, mode=Mode.MERGE,
                               group="item4 item19",
                               schema="stock_name last_price time",
                               first_idx=1, last_idx=2)

        expected_dict = {"user": "user1",
                         "sessionId": "Sc4a1769b6bb83a4aT2852044",
                         "table": table_info,
                         "mpn_subscription": subscription}

        self.assertDictEqual(expected_dict, self.collector)

    def test_notify_mpn_subscription_activation_APN_with_ne_exception(self):
        # Expect a NotificationError because NULL user is specified:
        # "c00000147c9bc4c74|MSA|S|#|..."
        self.do_init_and_skip()
        self.send_request(("c00000147c9bc4c74|MSA|S|#|S|"
                           "Sc4a1769b6bb83a4aT2852044|I|1|M|M|S|item4+item19|"
                           "S|stock_name+last_price+time|I|1|I|2|PA|2|1|P|A|S|"
                           "com.lightstreamer.demo.ios.stocklistdemo|S|"
                           "f74d8ffc5ee7cb31749a329a8f9202867c0a9906e80ee7f9ee"
                           "ccca1f24c5aaf1|S|"
                           "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+"
                           "1000.0|S|Default|S|AUTO|S|MARKET+VIEW|S|#|S|#|S|"
                           "MARKET+UPDATE|S|%24%7Bstock_name%7D|S|"
                           "%24%7Blast_price%7D|S|customKey1|S|123456"))
        self.assert_reply("c00000147c9bc4c74|MSA|EN|NotificationError")

    def test_notify_mpn_subscription_activation_APN_with_ce_exception(self):
        # Expect a NotificationError because EMPTY user is specified:
        # "c00000147c9bc4c74|MSA|S|$|..."
        self.do_init_and_skip()
        self.send_request(("c00000147c9bc4c74|MSA|S|$|S|"
                           "Sc4a1769b6bb83a4aT2852044|I|1|M|M|S|item4+item19|"
                           "S|stock_name+last_price+time|I|1|I|2|PA|2|1|P|A|S|"
                           "com.lightstreamer.demo.ios.stocklistdemo|S|"
                           "f74d8ffc5ee7cb31749a329a8f9202867c0a9906e80ee7f9ee"
                           "ccca1f24c5aaf1|S|"
                           "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+"
                           "1000.0|S|Default|S|AUTO|S|MARKET+VIEW|S|#|S|#|S|"
                           "MARKET+UPDATE|S|%24%7Bstock_name%7D|S|"
                           "%24%7Blast_price%7D|S|customKey1|S|123456"))
        self.assert_reply(("c00000147c9bc4c74|MSA|EC|CreditsError|10|"
                           "clientErrorMsg"))

    def test_notify_mpn_subscription_activation_APN_with_exception(self):
        # Expect a NotificationError because "user2" user is specified:
        # "c00000147c9bc4c74|MSA|S|user2|..."
        self.do_init_and_skip()
        self.send_request(("c00000147c9bc4c74|MSA|S|user2|S|"
                           "Sc4a1769b6bb83a4aT2852044|I|1|M|M|S|item4+item19|"
                           "S|stock_name+last_price+time|I|1|I|2|PA|2|1|P|A|S|"
                           "com.lightstreamer.demo.ios.stocklistdemo|S|"
                           "f74d8ffc5ee7cb31749a329a8f9202867c0a9906e80ee7f9ee"
                           "ccca1f24c5aaf1|S|"
                           "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+"
                           "1000.0|S|Default|S|AUTO|S|MARKET+VIEW|S|#|S|#|S|"
                           "MARKET+UPDATE|S|%24%7Bstock_name%7D|S|"
                           "%24%7Blast_price%7D|S|customKey1|S|123456"))
        self.assert_reply("c00000147c9bc4c74|MSA|E|Exception")

    def test_notify_mpn_subscription_activation_GCM(self):
        self.do_init_and_skip()
        request = ("c00000147c9bc4c74|MSA|S|user1|S|Sc4a1769b6bb83a4aT2852044|"
                   # I|<win. index>|M|<pub. mode>|S|<item group>|S|
                   # <field schema>|
                   "I|1|M|M|S|item4+item19|S|stock_name+last_price+time|"
                   # I|<first item idx.>|I|<last item idx.>|
                   "I|1|I|2|"
                   # PG|<num. of test_data>|
                   "PG|3|"
                   # P|G|S|<application ID>|S|<device token>|S|<trigger>|
                   "P|G|S|com.lightstreamer.demo.android.stocklistdemo|S|"
                   "2082055669|S|"
                   "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+1000.0|"
                   # S|<collapse key>|
                   "S|Stock+update|"
                   # S|<test_data key 1>|S|<value key 1>|...|S|
                   # <test_data key N>|S|<value key N>|
                   "S|time|S|%24%7Btime%7D|S|last_price|S|"
                   "%24%7Blast_price%7D|S|stock_name|S|%24%7Bstock_name%7D|"
                   # S|<delay while idle>|S|<time to live>
                   "S|false|S|30")
        self.send_request(request)
        self.assert_reply("c00000147c9bc4c74|MSA|V")

        table_info = TableInfo(win_index=1, mode=Mode.MERGE,
                               group="item4 item19",
                               schema="stock_name last_price time",
                               first_idx=1, last_idx=2)
        device = MpnDeviceInfo(
                 platform_type=MpnPlatformType.GCM,
                 application_id="com.lightstreamer.demo.android.stocklistdemo",
                 device_token="2082055669")

        subscription = MpnGcmSubscriptionInfo(
                        device=device,
                        trigger="Double.parseDouble(${last_price}) > 1000.0",
                        collapse_key='Stock update',
                        delay_while_idle='false',
                        time_to_live='30',
                        data={'time': '${time}', 'last_price': '${last_price}',
                              'stock_name': '${stock_name}'})

        expected_dict = {"user": "user1",
                         "sessionId": "Sc4a1769b6bb83a4aT2852044",
                         "table": table_info,
                         "mpn_subscription": subscription}
        self.assertDictEqual(expected_dict, self.collector)

    def test_notify_mpn_subscription_activation_GCM_with_ne_exception(self):
        self.do_init_and_skip()
        self.send_request(("c00000147cac69643|MSA|S|#|S|"
                           "S401e2449d3b79feT1213883|I|1|M|M|S|item4+item19|S|"
                           "stock_name+last_price+time|I|1|I|2|PG|3|P|G|S|"
                           "com.lightstreamer.demo.android.stocklistdemo|S|"
                           "2082055669|S|Double.parseDouble%28%24%7Blast_price"
                           "%7D%29+%3E+1000.0|S|Stock+update|S|time|S|"
                           "%24%7Btime%7D|S|last_price|S|%24%7Blast_price%7D|"
                           "S|stock_name|S|%24%7Bstock_name%7D|S|false|S|30"))
        self.assert_reply("c00000147cac69643|MSA|EN|NotificationError")

    def test_notify_mpn_subscription_activation_GCM_with_ce_exception(self):
        self.do_init_and_skip()
        self.send_request(("c00000147cac69643|MSA|S|$|S|"
                           "S401e2449d3b79feT1213883|I|1|M|M|S|item4+item19|S|"
                           "stock_name+last_price+time|I|1|I|2|PG|3|P|G|S|"
                           "com.lightstreamer.demo.android.stocklistdemo|S|"
                           "2082055669|S|"
                           "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+"
                           "1000.0|S|Stock+update|S|time|S|%24%7Btime%7D|S|"
                           "last_price|S|%24%7Blast_price%7D|S|stock_name|S|"
                           "%24%7Bstock_name%7D|S|false|S|30"))
        self.assert_reply(("c00000147cac69643|MSA|EC|CreditsError|10|"
                           "clientErrorMsg"))

    def test_notify_mpn_subscription_activation_GCM_with_exception(self):
        self.do_init_and_skip()
        self.send_request(("c00000147cac69643|MSA|S|user2|S"
                           "|S401e2449d3b79feT1213883|I|1|M|M|S|item4+item19"
                           "|S|stock_name+last_price+time|I|1|I|2|PG|3|P|G|S|"
                           "com.lightstreamer.demo.android.stocklistdemo|S|"
                           "2082055669|S|"
                           "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+"
                           "1000.0|S|Stock+update|S|time|S|%24%7Btime%7D|S|"
                           "last_price|S|%24%7Blast_price%7D|S|stock_name|S|"
                           "%24%7Bstock_name%7D|S|false|S|30"))
        self.assert_reply("c00000147cac69643|MSA|E|Exception")

    def test_malformed_mpn_subscription_activation(self):
        self.do_init_and_skip()
        # Invalid "PH" token as mpn subcription type
        request = ("c00000147c9bc4c74|MSA|S|user1|S|Sc4a1769b6bb83a4aT2852044|"
                   # I|<win. index>|M|<pub. mode>|S|<item group>|S|
                   # <field schema>|
                   "I|1|M|M|S|item4+item19|S|stock_name+last_price+time|"
                   # I|<first item idx.>|I|<last item idx.>|
                   "I|1|I|2|"
                   # PG|<num. of test_data>|
                   "PH|3")
        self.send_request(request)
        self.assert_caught_exception(("Unsupported MPN subscription type while"
                                      " parsing a MSA request"))

        # Missing next token to PG
        request = ("c00000147c9bc4c74|MSA|S|user1|S|Sc4a1769b6bb83a4aT2852044|"
                   # I|<win. index>|M|<pub. mode>|S|<item group>|S|
                   # <field schema>|
                   "I|1|M|M|S|item4+item19|S|stock_name+last_price+time|"
                   # I|<first item idx.>|I|<last item idx.>|
                   "I|1|I|2|"
                   # PG|<num. of test_data>|
                   "PG|")
        self.send_request(request)
        self.assert_caught_exception(("Token not found while parsing a MSA "
                                      "request"))

        # Missing next token to PA
        request = ("c00000147c9bc4c74|MSA|S|user1|S|Sc4a1769b6bb83a4aT2852044|"
                   # I|<win. index>|M|<pub. mode>|S|<item group>|S|
                   # <field schema>|
                   "I|1|M|M|S|item4+item19|S|stock_name+last_price+time|"
                   # I|<first item idx.>|I|<last item idx.>|
                   "I|1|I|2|"
                   # PG|<num. of test_data>|
                   "PA|")
        self.send_request(request)
        self.assert_caught_exception(("Token not found while parsing a MSA "
                                      "request"))

        # Invalid next literal token to "PA"
        request = ("c00000147c9bc4c74|MSA|S|user1|S|Sc4a1769b6bb83a4aT2852044|"
                   # I|<win. index>|M|<pub. mode>|S|<item group>|S|
                   # <field schema>|
                   "I|1|M|M|S|item4+item19|S|stock_name+last_price+time|"
                   # I|<first item idx.>|I|<last item idx.>|
                   "I|1|I|2|"
                   # PG|<num. of test_data>|
                   "PA|s")
        self.send_request(request)
        self.assert_caught_exception(("An unexpected exception caught while "
                                      "parsing a MSA request"))

        # Invalid "H" token as mode type
        request = ("c00000147c9bc4c74|MSA|S|user1|S|Sc4a1769b6bb83a4aT2852044|"
                   # I|<win. index>|M|<pub. mode>|S|<item group>|S|
                   # <field schema>|
                   "I|1|M|H|S|item4+item19|S|stock_name+last_price+time|"
                   # I|<first item idx.>|I|<last item idx.>|
                   "I|1|I|2|"
                   # PG|<num. of test_data>|
                   "PG|3|"
                   # P|G|S|<application ID>|S|<device token>|S|<trigger>|
                   "P|G|S|com.lightstreamer.demo.android.stocklistdemo|S|"
                   "2082055669|S|"
                   "Double.parseDouble%28%24%7Blast_price%7D%29+%3E+1000.0|"
                   # S|<collapse key>|
                   "S|Stock+update|"
                   # S|<test_data key 1>|S|<value key 1>|...|S|
                   # <test_data key N>|S|<value key N>|
                   "S|time|S|%24%7Btime%7D|S|last_price|S|%24%7Blast_price%7D|"
                   "S|stock_name|S|%24%7Bstock_name%7D|"
                   # S|<delay while idle>|S|<time to live>
                   "S|false|S|30")
        self.send_request(request)
        self.assert_caught_exception(("Unknown mode 'H' found while parsing a"
                                      " MSA request"))

    def test_notify_mpn_device_token_change(self):
        self.do_init_and_skip()
        self.send_request(("3700000147c9bc4c74|MDC|S|user1|P|A|S|"
                           "com.lightstreamer.demo.ios.stocklistdemo|S|"
                           "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                           "24cff14a9906f1|S|0849781a0afe0311f58bbfee1fcde031b"
                           "fc56635c89c566dda3c6708fd893549"))
        self.assert_reply("3700000147c9bc4c74|MDC|V")
        device = MpnDeviceInfo(
                     platform_type=MpnPlatformType.APNS,
                     application_id="com.lightstreamer.demo.ios.stocklistdemo",
                     device_token=("f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c"
                                   "09e202ec24cff14a9906f1"))

        expected_dict = {"user": "user1",
                         "mpnDeviceInfo": device,
                         "newDeviceToken": ("0849781a0afe0311f58bbfee1fcde031b"
                                            "fc56635c89c566dda3c6708fd893549")}
        self.assertDictEqual(expected_dict, self.collector)

    def test_notify_mpn_device_token_change_with_null_platform_type(self):
        self.do_init_and_skip()
        self.send_request(("3700000147c9bc4c74|MDC|S|user1|P|#|S|"
                           "com.lightstreamer.demo.ios.stocklistdemo|S|"
                           "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                           "24cff14a9906f1|S|0849781a0afe0311f58bbfee1fcde031b"
                           "fc56635c89c566dda3c6708fd893549"))
        self.assert_reply("3700000147c9bc4c74|MDC|V")
        device = MpnDeviceInfo(platform_type=None,
                               application_id=("com.lightstreamer.demo.ios."
                                               "stocklistdemo"),
                               device_token=("f780e9d8ffc86a5ec9a329e7745aa8fb"
                                             "3a1ecce77c""09e202ec24cff14a9906"
                                             "f1"))
        expected_dict = {"user": "user1",
                         "mpnDeviceInfo": device,
                         "newDeviceToken": ("0849781a0afe0311f58bbfee1fcde031b"
                                            "fc56635c89c566dda3c6708fd893549")}
        self.assertDictEqual(expected_dict, self.collector)

    def test_notify_mpn_device_token_change_with_empty_platform_type(self):
        self.do_init_and_skip()
        self.send_request(("3700000147c9bc4c74|MDC|S|user1|P|$|S|"
                           "com.lightstreamer.demo.ios.stocklistdemo|S|"
                           "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                           "24cff14a9906f1|S|0849781a0afe0311f58bbfee1fcde031b"
                           "fc56635c89c566dda3c6708fd893549"))
        self.assert_reply("3700000147c9bc4c74|MDC|V")

        device = MpnDeviceInfo(platform_type="",
                               application_id=("com.lightstreamer.demo.ios."
                                               "stocklistdemo"),
                               device_token=("f780e9d8ffc86a5ec9a329e7745aa8fb"
                                             "3a1ecce77c""09e202ec24cff14a9906"
                                             "f1"))
        expected_dict = {"user": "user1",
                         "mpnDeviceInfo": device,
                         "newDeviceToken": ("0849781a0afe0311f58bbfee1fcde031b"
                                            "fc56635c89c566dda3c6708fd893549")}
        self.assertDictEqual(expected_dict, self.collector)

    def test_notify_mpn_device_token_change_with_notification_exception(self):
        self.do_init_and_skip()
        self.send_request(("3700000147c9bc4c74|MDC|S|#|P|A|S|"
                           "com.lightstreamer.demo.ios.stocklistdemo|S|"
                           "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                           "24cff14a9906f1|S|0849781a0afe0311f58bbfee1fcde031b"
                           "fc56635c89c566dda3c6708fd893549"))
        self.assert_reply("3700000147c9bc4c74|MDC|EN|NotificationError")

    def test_notify_mpn_device_token_change_with_credits_exception(self):
        self.do_init_and_skip()
        self.send_request(("3700000147c9bc4c74|MDC|S|$|P|A|S|"
                           "com.lightstreamer.demo.ios.stocklistdemo|S|"
                           "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                           "24cff14a9906f1|S|0849781a0afe0311f58bbfee1fcde031b"
                           "fc56635c89c566dda3c6708fd893549"))
        self.assert_reply(("3700000147c9bc4c74|MDC|EC|CreditsError|10|"
                           "clientErrorMsg"))

    def test_notify_mpn_device_token_change_with_generic_exception(self):
        self.do_init_and_skip()
        self.send_request(("3700000147c9bc4c74|MDC|S|user2|P|A|S|"
                           "com.lightstreamer.demo.ios.stocklistdemo|S|"
                           "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                           "24cff14a9906f1|S|0849781a0afe0311f58bbfee1fcde031b"
                           "fc56635c89c566dda3c6708fd893549"))
        self.assert_reply("3700000147c9bc4c74|MDC|E|Exception")

    def test_malformed_notify_mpn_device_token_change(self):
        self.do_init_and_skip()
        self.send_request(("3700000147c9bc4c74|MDC|S|user1|P|B1|S|"
                           "com.lightstreamer.demo.ios.stocklistdemo|S|"
                           "f780e9d8ffc86a5ec9a329e7745aa8fb3a1ecce77c09e202ec"
                           "24cff14a9906f1|S|"
                           "0849781a0afe0311f58bbfee1fcde031bfc56635c89c566dda"
                           "3c6708fd893549"))
        self.assert_caught_exception(("Unknown platform type 'B1' while "
                                      "parsing a MDC request"))


if __name__ == "__main__":
    # import syssys.argv = ['', 'DataProviderTest.testName']
    unittest.main()
