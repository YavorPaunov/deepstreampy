"""Tests for parsing and building messages and converting parsed messages."""
from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.message import message_builder, message_parser
from deepstreampy.constants import topic, actions, types
from deepstreampy.constants import topic as topic_constants
from deepstreampy.constants import event as event_constants
from deepstreampy import client

import json
import unittest

URL = "ws://localhost:7777/deepstream"

class MessageTest(unittest.TestCase):

    def setUp(self):
        # This client will never connect
        self.client = client.Client(URL)

    def test_message_build_and_parse(self):
        """Test building messages and parsing them back correctly."""
        data = [{'username': 'bob'}]
        real_message = message_builder.get_message(topic.AUTH,
                                                   actions.REQUEST,
                                                   data)
        expected_message = (
            topic.AUTH + chr(31) + actions.REQUEST + chr(31) +
            json.dumps(data[0], separators=(',', ':')) + chr(30))
        self.assertEqual(real_message, expected_message)
        parsed_messages = message_parser.parse(real_message, self.client)
        self.assertEqual(len(parsed_messages), 1)
        message = parsed_messages[0]
        self.assertEqual(message['topic'], topic.AUTH)
        self.assertEqual(message['action'], actions.REQUEST)
        self.assertEqual([json.loads(message['data'][0])], data)
        data = ['entry1', 'entry2']
        real_message = message_builder.get_message(topic.EVENT,
                                                   actions.READ,
                                                   data)
        expected_message = (topic.EVENT + chr(31) +
                            actions.READ + chr(31) +
                            data[0] + chr(31) +
                            data[1] + chr(30))
        self.assertEqual(real_message, expected_message)

        parsed_messages = message_parser.parse(real_message, self.client)
        self.assertEqual(len(parsed_messages), 1)

        message = parsed_messages[0]
        self.assertEqual(message['topic'], topic.EVENT)
        self.assertEqual(message['action'], actions.READ)
        self.assertEqual(message['data'], data)

    def test_wrong_action(self):
        """Test parsing message with an action that doesn't exist."""
        self.assertRaises(ValueError,
                          message_parser.parse,
                          topic.AUTH + chr(31) + 'F' + chr(30),
                          self.client)

    def test_parse_short_message(self):
        """Test parsing message with too few parts."""
        self.assertRaises(ValueError,
                          message_parser.parse,
                          topic.AUTH + chr(30),
                          self.client)

    def test_to_typed(self):
        """Test convert to typed."""
        self.assertEqual(message_builder.typed("somestring"),
                         types.STRING + "somestring")
        self.assertEqual(message_builder.typed(1), "N1")
        self.assertEqual(message_builder.typed(2.3), "N2.3")
        self.assertEqual(message_builder.typed(True), "T")
        self.assertEqual(message_builder.typed(False), "F")
        self.assertEqual(message_builder.typed(None), "L")
        object_message = message_builder.typed(
            {"foo": 2, "bar": True, 'a': 'b'})
        self.assertEqual(object_message[0], 'O')
        self.assertTrue('"foo":2' in object_message)
        self.assertTrue('"bar":true' in object_message)
        self.assertTrue('"a":"b"' in object_message)

    def test_from_typed(self):
        """Test convert from typed."""
        self.assertEqual(
            message_parser.convert_typed("Ssomestring", self.client),
            "somestring")

        self.assertEqual(message_parser.convert_typed("N1", self.client), 1)
        self.assertRaises(ValueError,
                          message_parser.convert_typed,
                          "X21323",
                          self.client)

        self.client.once('error', self._handle_unknown_type)
        message_parser.convert_typed("X21323", self.client)

        self.client.once('error', self._handle_faulty_object)
        message_parser.convert_typed("O<21323>", self.client)

    def _handle_unknown_type(self, message, event, topic):
        self.assertEqual(message, "UNKNOWN_TYPE (X21323)")
        self.assertEqual(topic, topic_constants.ERROR)
        self.assertEqual(event, event_constants.MESSAGE_PARSE_ERROR)

    def _handle_faulty_object(self, message, event, topic):
        self.assertTrue("JSON object", message)
        self.assertEqual(topic, topic_constants.ERROR)
        self.assertEqual(event, event_constants.MESSAGE_PARSE_ERROR)

if __name__ == '__main__':
    unittest.main()
