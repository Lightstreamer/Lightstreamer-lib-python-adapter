"""
Module for testing the ARI Data Provider protocol implemented in the
Lightstreamer SDK for Python Adapters.
"""
import unittest
import base64
from collections import OrderedDict
from lightstreamer_adapter import data_protocol
from lightstreamer_adapter.protocol import RemotingException


class DataProtocolTest(unittest.TestCase):
    """TestCase for the Data Provider protocol.
    """

    def test_ud3_empty_map(self):
        """Tests the response to an UD3 request with an empty update
        dictionary.
        """
        events_map = {}
        res = data_protocol.write_update_map("item1", "10000010c3e4d0462",
                                             False, events_map)
        self.assertEqual(("UD3|S|item1|S|10000010c3e4d0462|B|0"), res)

    def test_ud3_none_map(self):
        """Tests the response to an UD3 request with a None update dictionary.
        """
        events_map = None
        res = data_protocol.write_update_map("item1", "10000010c3e4d0462",
                                             False, events_map)
        self.assertEqual(("UD3|S|item1|S|10000010c3e4d0462|B|0"), res)

    def test_ud3_one_pair_map(self):
        """Tests the response to an UD3 request with a single key-value pair.
        """
        events_map = {"field1": "value1"}

        res = data_protocol.write_update_map("item1", "10000010c3e4d0462",
                                             False, events_map)
        self.assertEqual(("UD3|S|item1|S|10000010c3e4d0462|B|0|S|field1|S"
                          "|value1"), res)

    def test_ud3_empty_value(self):
        """Tests the response to an UD3 request with a single key-value pair,
        where the value is an empty string.
        """
        events_map = {"field1": ""}

        res = data_protocol.write_update_map("item1", "10000010c3e4d0462",
                                             False, events_map)
        self.assertEqual("UD3|S|item1|S|10000010c3e4d0462|B|0|S|field1|S|$",
                         res)

    def test_ud3_none_value(self):
        """Tests the response to an UD3 request with a single key-value pair,
        where the value is None.
        """
        events_map = {"field1": None}

        res = data_protocol.write_update_map("item1", "10000010c3e4d0462",
                                             False, events_map)
        self.assertEqual("UD3|S|item1|S|10000010c3e4d0462|B|0|S|field1|S|#",
                         res)

    def test_ud3_to_be_quoted_spaces(self):
        """Tests the response to an UD3 request with a single key-value pair,
        where the value is space separated.
        """
        events_map = {"field1": "a long value"}

        res = data_protocol.write_update_map("item1", "10000010c3e4d0462",
                                             False, events_map)
        self.assertEqual(("UD3|S|item1|S|10000010c3e4d0462|B|0|S|field1|S|"
                          "a+long+value"), res)

    def test_ud3_to_be_quoted_symbol(self):
        """Tests the response to an UD3 request with a single key-value pair,
        where the value contains the symbol '@'.
        """
        events_map = {"field1": "A symbol to encode ©"}

        res = data_protocol.write_update_map("item1", "10000010c3e4d0462",
                                             False, events_map)
        self.assertEqual(("UD3|S|item1|S|10000010c3e4d0462|B|0|S|field1|S|"
                          "A+symbol+to+encode+%C2%A9"), res)

    def test_ud3_byte_value(self):
        """Tests the response to an UD3 request with a single key-value pair,
        where the value is a byte string.
        """
        events_map = {"field1": b'value of the field'}
        res = data_protocol.write_update_map("item1", "10000010c3e4d0462",
                                             False, events_map)

        encoded_value = base64.b64encode(b'value of the field').decode('utf-8')
        self.assertEqual("UD3|S|item1|S|10000010c3e4d0462|B|0|S|field1|Y|{}".
                         format(encoded_value), res)

    def test_ud3_wrong_value_type(self):
        """Tests the response to an UD3 request with a single key-value pair,
        where the value is of a wrong type.
        """
        events_map = {"field1": 4}
        with self.assertRaises(RemotingException) as err:
            data_protocol.write_update_map("item1", "10000010c3e4d0462",
                                           False, events_map)

        self.assertEqual(("Found value '4' of an unsupported type while "
                          "building a UD3 request"), str(err.exception))

    def test_ud3_more_pairs_map(self):
        """Tests the response to an UD3 request with more than only one
        key-value pair.
        """
        events_map = OrderedDict([("field1", "value1"),
                                  ("field2", "value2")])

        res = data_protocol.write_update_map("item1", "10000010c3e4d0462",
                                             False, events_map)
        self.assertEqual(("UD3|S|item1|S|10000010c3e4d0462|B|0|S|field1|S"
                          "|value1|S|field2|S|value2"), res)

    def test_ud3_wrong_bool_type_as_int(self):
        """Tests the response to an UD3 request with an item for which the
        Is Snapshot flag is of wrong type (int).
        """
        with self.assertRaises(RemotingException) as err:
            data_protocol.write_update_map("item1", "10000010c3e4d0462",
                                           4, {})

        self.assertEqual("Not a bool value: '4'", str(err.exception))

    def test_ud3_missing_value(self):
        """Tests the response to an UD3 request with an item for which the
        Request ID Is Snapshot flag is not specified (None).
        """
        # It is not useful to test cases where Item Name or Request Id are
        # None, as this case is protected by the DataProviderServer.
        with self.assertRaises(RemotingException) as err:
            data_protocol.write_update_map("item1", "10000010c3e4d0462", None,
                                           {})

        self.assertEqual("Not a bool value: 'None'", str(err.exception))
