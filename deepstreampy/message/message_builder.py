from __future__ import absolute_import, division, print_function, with_statement
from deepstreampy.constants import types
from deepstreampy.constants import message as message_constants
from deepstreampy.utils import Undefined
import sys
import json


def get_message(topic, action, data=None):
    send_data = [topic, action]

    if data:
        for param in data:
            if isinstance(param, dict):
                value = json.dumps(param,
                                   separators=(',', ':'),
                                   sort_keys=True)
                send_data.append(value)
            elif isinstance(param, list):
                value = ("[" +
                         ",".join('"{0}"'.format(item) for item in param) +
                         "]")
                send_data.append(value)
            else:
                send_data.append(str(param))

    full_message = (message_constants.MESSAGE_PART_SEPERATOR.join(send_data) +
                    message_constants.MESSAGE_SEPERATOR)
    return full_message


def typed(value):
    if value is None:
        return types.NULL

    value_type = type(value)

    if sys.version_info < (3,):
        num_types = (int, long, float, complex)
        str_types = (str, unicode)
    else:
        num_types = (int, float, complex)
        str_types = (str,)

    if value_type in str_types:
        return types.STRING + value

    if value_type in [dict, list]:
        return types.OBJECT + json.dumps(value,
                                         separators=(',', ':'),
                                         sort_keys=False)

    if value_type is bool:
        if value:
            return types.TRUE
        else:
            return types.FALSE

    if value_type in num_types:
        return types.NUMBER + str(value)

    if value is Undefined:
        return types.UNDEFINED

    raise ValueError("Can't serialize type {0}".format(value_type))
