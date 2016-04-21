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


class MessageTest(unittest.TestCase):

    def setUp(self):
        # This client will never connect
        self.client = client.Client('localhost', 0)

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
        self.assertEquals(len(parsed_messages), 1)
        message = parsed_messages[0]
        self.assertEquals(message['topic'], topic.AUTH)
        self.assertEquals(message['action'], actions.REQUEST)
        self.assertEquals([json.loads(message['data'][0])], data)
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
        self.assertEquals(len(parsed_messages), 1)

        message = parsed_messages[0]
        self.assertEquals(message['topic'], topic.EVENT)
        self.assertEquals(message['action'], actions.READ)
        self.assertEquals(message['data'], data)

    def test_wrong_action(self):
        """Test parsing message with an action that doesn't exist."""
        with self.assertRaises(ValueError):
            message_parser.parse(topic.AUTH + chr(31) + 'F' + chr(30),
                                 self.client)

    def test_parse_short_message(self):
        """Test parsing message with too few parts."""
        with self.assertRaises(ValueError):
            message_parser.parse(topic.AUTH + chr(30), self.client)

    def test_to_typed(self):
        """Test convert to typed."""
        self.assertEquals(message_builder.typed("somestring"),
                          types.STRING + "somestring")
        self.assertEquals(message_builder.typed(1), "N1")
        self.assertEquals(message_builder.typed(2.3), "N2.3")
        self.assertEquals(message_builder.typed(True), "T")
        self.assertEquals(message_builder.typed(False), "F")
        self.assertEquals(message_builder.typed(None), "L")
        object_message = message_builder.typed(
            {"foo": 2, "bar": True, 'a': 'b'})
        self.assertEquals(object_message[0], 'O')
        self.assertIn('"foo":2', object_message)
        self.assertIn('"bar":true', object_message)
        self.assertIn('"a":"b"', object_message)

    def test_from_typed(self):
        """Test convert from typed."""
        self.assertEquals(
            message_parser.convert_typed("Ssomestring", self.client),
            "somestring")

        self.assertEqual(message_parser.convert_typed("N1", self.client), 1)
        with self.assertRaises(ValueError):
            message_parser.convert_typed("X21323", self.client)

        self.client.once('error', self._handle_unknown_type)
        message_parser.convert_typed("X21323", self.client)

        self.client.once('error', self._handle_faulty_object)
        message_parser.convert_typed("O<21323>", self.client)

    def _handle_unknown_type(self, message, event, topic):
        self.assertEquals(message, "UNKNOWN_TYPE (X21323)")
        self.assertEquals(topic, topic_constants.ERROR)
        self.assertEquals(event, event_constants.MESSAGE_PARSE_ERROR)

    def _handle_faulty_object(self, message, event, topic):
        self.assertIn("JSON object", message)
        self.assertEquals(topic, topic_constants.ERROR)
        self.assertEquals(event, event_constants.MESSAGE_PARSE_ERROR)

if __name__ == '__main__':
    unittest.main()
