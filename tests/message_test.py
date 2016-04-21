"""Tests for parsing and building messages and converting parsed messages."""
from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.message import message_builder, message_parser
from deepstreampy.constants import topic, actions, types
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
            json.dumps(data[0], separators=(',', ':')))
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
        # message_parser.parse('')

    def test_to_typed(self):
        self.assertEquals(
            message_builder.typed("somestring"),
            types.STRING + "somestring")
        self.assertEquals(message_builder.typed(1), "N1")
        self.assertEquals(message_builder.typed(True), "T")
        self.assertEquals(message_builder.typed(False), "F")

    def test_from_typed(self):
        self.assertEquals(
            message_parser.convert_typed("Ssomestring", self.client),
            "somestring")
        self.assertEqual(message_parser.convert_typed("N1", client), 1)
        self.assertIs(message_parser.convert_typed("X21323", client), None)

if __name__ == '__main__':
    unittest.main()
