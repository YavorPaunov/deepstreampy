from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy.message import message_parser

from behave import given, when, then
from tornado import testing

import json
import sys
import config

if sys.version_info[0] < 3:
    import mock
else:
    from unittest import mock


@given(u'the test server is ready')
def server_ready(context):
    if not context.server:
        context.server = context.create_server(7777, '/deepstream')


@given(u'the second test server is ready')
def second_server_ready(context):
    if not context.other_server:
        context.other_server = context.create_server(8888, '/deepstream2')


@given(u'the server resets its message count')
def server_reset_count(context):
    del context.received_messages('/deepstream')[:]


@given(u'the server sends the message {message}')
@when(u'the server sends the message {message}')
@testing.gen_test
def server_sends_message(context, message):
    message = message.replace("<SECOND_SERVER_URL>", config.SECOND_SERVER_URL)
    message = message.replace("|", chr(31)).replace("+", chr(30))

    if (context.client and
            context.client._connection._url == config.SECOND_SERVER_URL):
        yield next(iter(
            context.connections('/deepstream2'))).write_message(message)
    else:
        yield next(iter(
            context.connections('/deepstream'))).write_message(message)


@then(u'the server has {num_connections} active connections')
def server_num_connections(context, num_connections):
    connections_count = context.num_connections('/deepstream')
    assert connections_count == int(num_connections), (str(connections_count) +
                                                       " active connections.")


@then(u'the second server has {num_connections} active connections')
def second_server_num_connections(context, num_connections):
    connections_count = context.num_connections('/deepstream2')
    assert connections_count == int(num_connections), (str(connections_count) +
                                                       " active connections.")


@then(u'the last message the server recieved is {message}')
def server_last_message(context, message):
    expected = message.replace("|", chr(31)).replace("+", chr(30))
    expected = expected.replace("<FIRST_SERVER_URL>", config.FIRST_SERVER_URL)
    parsed_expected = message_parser.parse(expected, context.client)[0]

    if context.client._connection._url == config.FIRST_SERVER_URL:
        messages = context.received_messages('/deepstream')
    else:
        messages = context.received_messages('/deepstream2')

    while True:
        if messages:
            parsed_actual = message_parser.parse(
                messages[-1], context.client)[0]
            if parsed_actual == parsed_expected:
                return

            same_topic = parsed_expected['topic'] == parsed_actual['topic']
            same_action = parsed_expected['action'] == parsed_actual['action']
            same_data = True

            if len(parsed_expected['data']) != len(parsed_actual['data']):
                same_data = False
            else:
                for i, item in enumerate(parsed_expected['data']):
                    if len(parsed_actual['data']) < (i - 1):
                        same_data = False
                        continue

                    try:
                        same_data = (
                            same_data and
                            json.loads(item) == json.loads(
                                parsed_actual['data'][i]))
                    except (TypeError, ValueError):
                        same_data = (same_data and
                                     item == parsed_actual['data'][i])

            if same_topic and same_action and same_data:
                return

        msg_future = next(iter(
            context.connections('/deepstream'))).message_future()
        yield msg_future

    raise AssertionError("{0} not the last received".format(message))


@then(u'the server has received {num_messages} messages')
def server_num_messages(context, num_messages):
    expected = int(num_messages.encode())
    actual = len(context.received_messages('/deepstream'))
    assert actual == expected,  (
        "The server has received " + str(actual) +
        " messages, not " + str(expected))


@given(u'the client checks if the server has the record "{record_name}"')
@testing.gen_test
def record_check_has(context, record_name):
    callback = mock.Mock()
    yield context.client.record.has(record_name, callback)
    context.has_callbacks[record_name] = callback


@then(u'the server didn\'t receive the message {message}')
def step_impl(context, message):
    expected = message.replace("|", chr(31)).replace("+", chr(30))
    all_messages = context.received_messages('/deepstream')
    assert expected not in all_messages, all_messages


@then(u'the server received the message {message}')
def received_message(context, message):
    expected = message.replace("|", chr(31)).replace("+", chr(30))

    while True:
        if expected in context.received_messages('/deepstream'):
            return

        msg_future = next(iter(
            context.connections('/deepstream'))).message_future()
        yield msg_future

    raise AssertionError("{0} not received".format(message))


@when(u'the connection to the server is lost')
def connection_lost(context):
    for conn in context.connections('/deepstream'):
        conn.close()
    del context.received_messages('/deepstream')[:]
    context.server.stop()


@when(u'the connection to the server is reestablished')
@testing.gen_test
def connection_reestablished(context):
    context.server.listen(7777)
    yield context.client.connect()


@then(u'the server did not recieve any messages')
def server_no_message(context):
    assert len(context.received_messages('/deepstream')) == 0


@then(u'the client is on the second server')
def client_on_second_server(context):
    assert context.client._connection._url == config.SECOND_SERVER_URL
