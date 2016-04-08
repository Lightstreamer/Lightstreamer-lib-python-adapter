'''
Module for testing the ARI Metadata Provider protocol implemented in the
Lightstreamer SDK for Python Adapters.
'''
import unittest
from lightstreamer_adapter import metadata_protocol
from lightstreamer_adapter.protocol import RemotingException
from lightstreamer_adapter.interfaces.metadata import (Mode,
                                                       MetadataProviderError,
                                                       CreditsError,
                                                       NotificationError,
                                                       ConflictingSessionError,
                                                       AccessError,
                                                       ItemsError,
                                                       SchemaError)


def build_test_git_item(modes, distinct_snapshot_length, min_freq):
    """Create a dictionary object with all required key-pair values, to be used
    for testing the GIT method.
    """
    return {"allowedModeList": modes,
            "distinctSnapshotLength": distinct_snapshot_length,
            "minSourceFrequency": min_freq}

def build_test_gui_item(modes, buf_size, max_freq):
    """Create a dictionary object with all required key-pair values, to be used
    for testing the GUI method.
    """
    return {"allowedModeList": modes,
            "allowedBufferSize": buf_size,
            "allowedMaxFrequency": max_freq}


class MetadataProtocolTest(unittest.TestCase):
    """TestCase for the Metadata Provider protocol.
    """

    def test_mpi(self):
        """Tests the response to a MPI request.
        """
        res = metadata_protocol.write_init()
        self.assertEqual("MPI|V", res)

    def test_mpi_metaproviderexception(self):
        """Tests the response to a MPI request in case of a
        MetadataProviderError.
        """
        error = MetadataProviderError("MetaProvider Error")
        res = metadata_protocol.write_init(error)
        self.assertEqual("MPI|EM|MetaProvider+Error", res)

    def test_mpi_generic_exception(self):
        """Tests the response to a MPI request in case of a generic exception.
        """
        res = metadata_protocol.write_init(RuntimeError("Generic Error"))
        self.assertEqual("MPI|E|Generic+Error", res)

    def test_nns(self):
        """Tests the response to a NNS request.
        """
        res = metadata_protocol.write_notify_new_session()
        self.assertEqual("NNS|V", res)

    def test_nns_notification_error(self):
        """Tests the response to a NNS request in case of a
        NotificationError.
        """
        error = NotificationError("error")
        res = metadata_protocol.write_notify_new_session(error)
        self.assertEqual("NNS|EN|error", res)

    def test_nns_credits_error(self):
        """Tests the response to a NNS request in case of a CreditsError.
        """
        error = CreditsError(12, "Credits Error", "Message Error")
        res = metadata_protocol.write_notify_new_session(error)
        self.assertEqual("NNS|EC|Credits+Error|12|Message+Error", res)

    def test_nns_conflict_session_error(self):
        """Tests the response to a NNS request in case of a
        ConflictingSessionError.
        """
        error = ConflictingSessionError(12, "Conflicting Session Error",
                                        "S1123", "User Message Error")
        res = metadata_protocol.write_notify_new_session(error)
        self.assertEqual(("NNS|EX|Conflicting+Session+Error|12|"
                          "User+Message+Error|S1123"), res)

    def test_nns_generic_exception(self):
        """Tests the response to a NNS request in case of a generic excpetion.
        """
        error = RuntimeError("Generic Error")
        res = metadata_protocol.write_notify_new_session(error)
        self.assertEqual("NNS|E|Generic+Error", res)

    def test_nus(self):
        """Tests the response to a NUS request.
        """
        allowed_max_bandwidth = 1.0
        wants_tables_notification = False
        res = metadata_protocol.write_notiy_user(metadata_protocol.Method.NUS,
                                                 allowed_max_bandwidth,
                                                 wants_tables_notification)
        self.assertEqual("NUS|D|1.0|B|0", res)

    def test_nua(self):
        """Tests the response to a NUA request.
        """
        allowed_max_bandwidth = 1.45
        wants_tables_notification = True
        res = metadata_protocol.write_notiy_user(metadata_protocol.Method.NUA,
                                                 allowed_max_bandwidth,
                                                 wants_tables_notification)
        self.assertEqual("NUA|D|1.45|B|1", res)

    def test_nus_missing_values(self):
        """Tests the response to a NUS request in case of a
        RemotingException because of missing values.
        """
        allowed_max_bandwidth = None
        wants_tables_notification = False
        with self.assertRaises(RemotingException) as err:
            metadata_protocol.write_notiy_user(metadata_protocol.Method.NUS,
                                               allowed_max_bandwidth,
                                               wants_tables_notification)
        self.assertEqual("Not a float value: 'None'", str(err.exception))

    def test_nus_wrong_bool_type(self):
        """Tests the response to a NUS request in case of a
        RemotingException because of error type when a bool is expected.
        """
        allowed_max_bandwidth = 1.45
        wants_tables_notification = 1
        with self.assertRaises(RemotingException) as err:
            metadata_protocol.write_notiy_user(metadata_protocol.Method.NUS,
                                               allowed_max_bandwidth,
                                               wants_tables_notification)
        self.assertEqual("Not a bool value: '1'", str(err.exception))

    def test_nus_access_error(self):
        """Tests the response to a NNS request in case of a AccessError.
        """
        error = AccessError("Access Error")
        res = metadata_protocol.write_notiy_user(metadata_protocol.Method.NUS,
                                                 exception=error)
        self.assertEqual("NUS|EA|Access+Error", res)

    def test_nus_credits_error(self):
        """Tests the response to a NNS request in case of a CreditsError.
        """
        error = CreditsError(14, "Credits Error")
        res = metadata_protocol.write_notiy_user(metadata_protocol.Method.NUS,
                                                 exception=error)
        self.assertEqual("NUS|EC|Credits+Error|14|#", res)

    def test_num(self):
        """Tests the response to a NUM request.
        """
        res = metadata_protocol.write_notify_user_message()
        self.assertEqual("NUM|V", res)

    def test_num_credits_error(self):
        """Tests the response to a NUM request in case of a CreditsError.
        """
        error = CreditsError(10, "Credits Error", "User Msg")
        res = metadata_protocol.write_notify_user_message(error)
        self.assertEqual("NUM|EC|Credits+Error|10|User+Msg", res)

    def test_num_notification_error(self):
        """Tests the response to a NUM request in case of a NotificationError.
        """
        error = NotificationError("Notification Error")
        res = metadata_protocol.write_notify_user_message(error)
        self.assertEqual("NUM|EN|Notification+Error", res)

    def test_num_generic_exception(self):
        """Tests the response to a NUM request in case of a generic exception.
        """
        error = RuntimeError("Generic Error")
        res = metadata_protocol.write_notify_user_message(error)
        self.assertEqual("NUM|E|Generic+Error", res)

    def test_nnt(self):
        """Tests the response to a NNT request.
        """
        res = metadata_protocol.write_notify_new_tables_data()
        self.assertEqual("NNT|V", res)

    def test_nsc(self):
        """Tests the response to a NSC request.
        """
        res = metadata_protocol.write_notify_session_close()
        self.assertEqual("NSC|V", res)

    def test_nsc_notification_error(self):
        """Tests the response to a NSC request in case of a NotficationError.
        """
        error = NotificationError("Notification Error")
        res = metadata_protocol.write_notify_session_close(error)
        self.assertEqual("NSC|EN|Notification+Error", res)

    def test_nsc_generic_exception(self):
        """Tests the response to a NSC request in case of a generic exception.
        """
        error = RuntimeError("Generic Error")
        res = metadata_protocol.write_notify_session_close(error)
        self.assertEqual("NSC|E|Generic+Error", res)

    def test_gis_empty(self):
        """Tests the response to a GIS request with an empty list of items.
        """
        items = []
        res = metadata_protocol.write_get_items(items)
        self.assertEqual("GIS", res)

    def test_gis_none(self):
        """Tests the response to a GIS request with a None list of items.
        """
        items = None
        res = metadata_protocol.write_get_items(items)
        self.assertEqual("GIS", res)

    def test_gis_one(self):
        """Tests the response to a GIS request with a single item.
        """
        items = ["item1"]
        res = metadata_protocol.write_get_items(items)
        self.assertEqual("GIS|S|item1", res)

    def test_gis_more(self):
        """Tests the response to a GIS request with a more than only one item.
        """
        items = ["item1", "item2"]
        res = metadata_protocol.write_get_items(items)
        self.assertEqual("GIS|S|item1|S|item2", res)

    def test_gis_to_be_quoted_spaces(self):
        """Tests the response to a GIS request with a single item whose name
        is space separated.
        """
        items = ["a long item name"]
        res = metadata_protocol.write_get_items(items)
        self.assertEqual("GIS|S|a+long+item+name", res)

    def test_gis_to_be_quoted_symbol(self):
        """Tests the response to a GIS request with a single item whose name
        contains the symbol '£'
        """
        items = ["A symbol to encode £"]
        res = metadata_protocol.write_get_items(items)
        self.assertEqual("GIS|S|A+symbol+to+encode+%C2%A3", res)

    def test_gis_tobe_quoted_from_bytes(self):
        """Tests the response to a GIS request with a single item whose name
        is expressed as a byte string.
        """
        items = [b'a long item name']
        res = metadata_protocol.write_get_items(items)
        self.assertEqual("GIS|S|a+long+item+name", res)

    def test_gis_none_string(self):
        """Tests the response to a GIS request with a list of wrong typed
        items.
        """
        items = [1, 2, 3]
        with self.assertRaises(RemotingException):
            metadata_protocol.write_get_items(items)

    def test_gis_items_error(self):
        """Tests the response to a GIS request in case of an ItemsError.
        """
        error = ItemsError("Items Error")
        res = metadata_protocol.write_get_items(exception=error)
        self.assertEqual("GIS|EI|Items+Error", res)

    def test_gis_generic_exception(self):
        """Tests the response to a GIS request in case of a generic exception.
        """
        error = RuntimeError("Generic Error")
        res = metadata_protocol.write_get_items(exception=error)
        self.assertEqual("GIS|E|Generic+Error", res)

    def test_git_empty(self):
        """Tests the response to a GIT request with an empty list of items.
        """
        items = []
        res = metadata_protocol.write_get_item_data(items)
        self.assertEqual("GIT", res)


    def test_git_none(self):
        """Tests the response to a GIT request with a None list of items.
        """
        items = None
        res = metadata_protocol.write_get_item_data(items)
        self.assertEqual("GIT", res)

    def test_git_one(self):
        """Tests the response to a GIT request with a single item.
        """
        items = [build_test_git_item([Mode.COMMAND], 3, 4.5)]
        res = metadata_protocol.write_get_item_data(items)
        self.assertEqual("GIT|I|3|D|4.5|M|C", res)

    def test_git_more(self):
        """Tests the response to a GIT request with a more than only one item.
        """
        items = [build_test_git_item([Mode.COMMAND], 3, 4.5),
                 build_test_git_item([Mode.DISTINCT], 1, 6.7)]
        res = metadata_protocol.write_get_item_data(items)
        self.assertEqual(("GIT"
                          "|I|3|D|4.5|M|C"
                          "|I|1|D|6.7|M|D"), res)

    def test_git_more_modes(self):
        """Tests the response to a GIT request with an item with more than
        only one Mode.
        """
        items = [build_test_git_item([Mode.COMMAND, Mode.RAW, Mode.MERGE], 3,
                                     4.5)]
        res = metadata_protocol.write_get_item_data(items)
        self.assertEqual("GIT|I|3|D|4.5|M|CRM", res)

    def test_git_missing_value(self):
        """Tests the response to a GIT request with an item for which the
        Distinct Snapshot Length is not specified (None).
        """
        items = [build_test_git_item([Mode.COMMAND, Mode.RAW, Mode.MERGE],
                                     None, 4.5)]
        with self.assertRaises(RemotingException) as err:
            metadata_protocol.write_get_item_data(items)

        self.assertEqual("Not an int value: 'None'", str(err.exception))

    def test_git_wrong_int_type(self):
        """Tests the response to a GIT request with an item for which the
        Distinct Snapshot Length is of a wrong type (str).
        """
        items = [build_test_git_item([Mode.COMMAND, Mode.RAW, Mode.MERGE],
                                     'A String', 4.5)]
        with self.assertRaises(RemotingException) as err:
            metadata_protocol.write_get_item_data(items)

        self.assertEqual("Not an int value: 'A String'",
                         str(err.exception))

    def test_git_wrong_int_type_as_bool(self):
        """Tests the response to a GIT request with an item for which the
        Distinct Snapshot Length is of a wrong type (bool).
        """
        items = [build_test_git_item([Mode.COMMAND, Mode.RAW, Mode.MERGE],
                                     True, 4.5)]
        with self.assertRaises(RemotingException) as err:
            metadata_protocol.write_get_item_data(items)

        self.assertEqual("Not an int value: 'True'",
                         str(err.exception))

    def test_git_wrong_float_type(self):
        """Tests the response to a GITS request with an item for which the
        Min Source Frequency is of a wrong type (int).
        """
        # Min Source Frequency as a int (not as a float)
        items = [build_test_git_item([Mode.COMMAND, Mode.RAW, Mode.MERGE], 3,
                                     4)]
        with self.assertRaises(RemotingException) as err:
            metadata_protocol.write_get_item_data(items)

        self.assertEqual("Not a float value: '4'", str(err.exception))

    def test_git_generic_error(self):
        """Tests the response to a GIT request in case of a generic exception.
        """
        error = RuntimeError("Generic Error")
        res = metadata_protocol.write_get_item_data(exception=error)
        self.assertEqual("GIT|E|Generic+Error", res)

    def test_gsc_empty(self):
        """Tests the response to a GSC request with an empty list of fields.
        """
        fields = []
        res = metadata_protocol.write_get_schema(fields)
        self.assertEqual("GSC", res)

    def test_gsc_none(self):
        """Tests the response to a GSC request with a None list of fields.
        """
        fields = None
        res = metadata_protocol.write_get_schema(fields)
        self.assertEqual("GSC", res)

    def test_gsc_one(self):
        """Tests the response to a GSC request with a single field.
        """
        fields = ["field1"]
        res = metadata_protocol.write_get_schema(fields)
        self.assertEqual("GSC|S|field1", res)

    def test_gsc_more(self):
        """Tests the response to a GSC request with more than only one field.
        """
        fields = ["field1", "field2"]
        res = metadata_protocol.write_get_schema(fields)
        self.assertEqual("GSC|S|field1|S|field2", res)

    def test_gsc_tobe_quoted_spaces(self):
        """Tests the response to a GSC request with a single fields whose name
        is space separated.
        """
        items = ["a long field name"]
        res = metadata_protocol.write_get_schema(items)
        self.assertEqual("GSC|S|a+long+field+name", res)

    def test_gsc_tobe_quoted_symbol(self):
        """Tests the response to a GSC request with a single item whose name
        contains the symbol '@'.
        """
        fields = ["A symbol to encode ž"]
        res = metadata_protocol.write_get_schema(fields)
        self.assertEqual("GSC|S|A+symbol+to+encode+%C5%BE", res)

    def test_gsc_tobe_quoted_from_bytes(self):
        """Tests the response to a GSC request with a single item whose name
        is expressed as a byte string.
        """
        fields = [b'a long field name']
        res = metadata_protocol.write_get_schema(fields)
        self.assertEqual("GSC|S|a+long+field+name", res)

    def test_gsc_none_string(self):
        """Tests the response to a GSC request with a list of wrong typed
        fields.
        """
        fields = [1, 2, 3]
        with self.assertRaises(RemotingException):
            metadata_protocol.write_get_schema(fields)

    def test_gsc_items_error(self):
        """Tests the response to a GSC request in case of an ItemsError.
        """
        error = ItemsError("Items Error")
        res = metadata_protocol.write_get_schema(exception=error)
        self.assertEqual("GSC|EI|Items+Error", res)

    def test_gsc_schema_error(self):
        """Tests the response to a GSC request in case of an ItemsError.
        """
        error = SchemaError("Schema Error")
        res = metadata_protocol.write_get_schema(exception=error)
        self.assertEqual("GSC|ES|Schema+Error", res)

    def test_gsc_generic_exception(self):
        """Tests the response to a GSC request in case of a generic exception.
        """
        error = RuntimeError("Generic Error")
        res = metadata_protocol.write_get_schema(exception=error)
        self.assertEqual("GSC|E|Generic+Error", res)

    def test_gui_empty(self):
        """Tests the response to a GUI request with an empty list of items.
        """
        items = []
        res = metadata_protocol.write_get_user_item_data(items)
        self.assertEqual("GUI", res)

    def test_gui_none(self):
        """Tests the response to a GUI request with a None list of items.
        """
        items = None
        res = metadata_protocol.write_get_user_item_data(items)
        self.assertEqual("GUI", res)

    def test_gui_one(self):
        """Tests the response to a GUI request with a single item.
        """
        items = [build_test_gui_item([Mode.COMMAND], 40, 56.0)]
        res = metadata_protocol.write_get_user_item_data(items)
        self.assertEqual("GUI|I|40|D|56.0|M|C", res)

    def test_gui_more(self):
        """Tests the response to a GUI request with a more than only one item.
        """
        items = [build_test_gui_item([Mode.COMMAND], 50, 56.0),
                 build_test_gui_item([Mode.DISTINCT], 20, 61.71)]
        res = metadata_protocol.write_get_user_item_data(items)
        self.assertEqual(("GUI"
                          "|I|50|D|56.0|M|C"
                          "|I|20|D|61.71|M|D"), res)

    def test_gui_more_modes(self):
        """Tests the response to a GUI request with an item with more than
        only one Mode.
        """
        items = [build_test_gui_item([Mode.COMMAND, Mode.RAW, Mode.MERGE], 31,
                                     24.5)]
        res = metadata_protocol.write_get_user_item_data(items)
        self.assertEqual("GUI|I|31|D|24.5|M|CRM", res)

    def test_gui_missing_value(self):
        """Tests the response to a GUI request with an item for which the
        Allowed Buffer Size is not specified (None).
        """
        items = [build_test_gui_item([Mode.COMMAND, Mode.RAW, Mode.MERGE],
                                     None, 24.5)]
        with self.assertRaises(RemotingException) as err:
            metadata_protocol.write_get_user_item_data(items)

        self.assertEqual("Not an int value: 'None'", str(err.exception))

    def test_gui_wrong_int_type(self):
        """Tests the response to a GUI request with an item for which the
        Allowed Buffer Size is of wrong type (str).
        """
        items = [build_test_gui_item([Mode.COMMAND, Mode.RAW, Mode.MERGE],
                                     'A String', 24.5)]
        with self.assertRaises(RemotingException) as err:
            metadata_protocol.write_get_user_item_data(items)

        self.assertEqual("Not an int value: 'A String'",
                         str(err.exception))

    def test_gui_wrong_int_type_as_bool(self):
        """Tests the response to a GUI request with an item for which the
        Allowed Buffer Size is of wrong type (bool).
        """
        items = [build_test_gui_item([Mode.COMMAND, Mode.RAW, Mode.MERGE],
                                     True, 24.5)]
        with self.assertRaises(RemotingException) as err:
            metadata_protocol.write_get_user_item_data(items)

        self.assertEqual("Not an int value: 'True'",
                         str(err.exception))

    def test_gui_wrong_float_type(self):
        """Tests the response to a GUI request with an item for which the
        Max Allowed Frequency is of wrong type (int).
        """
        # Min Source Frequency as a int (not as a float)
        items = [build_test_gui_item([Mode.COMMAND, Mode.RAW, Mode.MERGE], 31,
                                     40)]
        with self.assertRaises(RemotingException) as err:
            metadata_protocol.write_get_user_item_data(items)

        self.assertEqual("Not a float value: '40'", str(err.exception))

    def test_gui_generic_error(self):
        """Tests the response to a GUI request in case of a generic exception.
        """
        error = RuntimeError("Generic Error")
        res = metadata_protocol.write_get_user_item_data(exception=error)
        self.assertEqual("GUI|E|Generic+Error", res)