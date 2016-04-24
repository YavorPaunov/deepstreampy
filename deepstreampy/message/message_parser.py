from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.constants import message as message_constants
from deepstreampy.constants import topic, event, actions, types

import json
import sys


def parse(raw_messages, client):
    parsed_messages = []
    raw_messages = raw_messages.split(message_constants.MESSAGE_SEPERATOR)
    for msg in raw_messages:
        # Ensure msg is not an empty string
        if msg:
            parsed_messages.append(_parse_message(msg, client))
    return parsed_messages


def _parse_message(message, client):
    parts = message.split(message_constants.MESSAGE_PART_SEPERATOR)
    if len(parts) < 2:
        # A valid message consists of at least 2 parts: action and topic
        client._on_error(topic.ERROR,
                         event.MESSAGE_PARSE_ERROR,
                         'Insufficient message parts')
        return

    if not actions.reverse_lookup(parts[1]):
        client._on_error(topic.ERROR, event.MESSAGE_PARSE_ERROR,
                         'Unknown action {0}'.format(parts[1]))
        return

    return {'raw': message,
            'topic': parts[0],
            'action': parts[1],
            'data': parts[2:]}


def convert_typed(value, client):
    value_type = value[0]

    if value_type == types.STRING:
        return value[1:]

    if value_type == types.OBJECT:
        try:
            return json.loads(value[1:])
        except ValueError as e:
            client._on_error(topic.ERROR, event.MESSAGE_PARSE_ERROR, str(e))
            return

    if value_type == types.NUMBER:
        if sys.version_info < (3,):
            num_types = (int, long, float, complex)
        else:
            num_types = (int, float, complex)

        for num_type in num_types:
            try:
                return num_type(value[1:])
            except ValueError:
                pass

    client._on_error(topic.ERROR, event.MESSAGE_PARSE_ERROR,
                     'UNKNOWN_TYPE ({0})'.format(value))
