from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import client
from deepstreampy.message import message_parser
from tests.util import FakeServer

from behave import *
from tornado import testing

PORT = 6030


@given(u'the test server is ready')
def server_ready(context):
    server = FakeServer()
    server.listen(PORT)
    context.server = server


@given(u'the client is initialised')
@testing.gen_test
def client_init(context):
    context.client = client.Client("localhost", PORT)

    def error_callback(message, event, t):
        context.client_errors.append(dict(message=message,
                                          event=event,
                                          topic=t))

    context.client.on('error', error_callback)
    yield context.client.connect()


@given(u'the server resets its message count')
def server_reset_count(context):
    context.server.reset_message_count()


@given(u'the client logs in with username "{username}" and password '
       '"{password}"')
@when(u'the client logs in with username "{username}" and password '
      '"{password}"')
@testing.gen_test
def client_login(context, username, password):
    auth_data = dict(username=username, password=password)
    server_future = context.server.await_message()
    context.login_future = context.client.login(auth_data)
    yield server_future


@given(u'the server sends the message "{message}"')
@when(u'the server sends the message "{message}"')
@testing.gen_test
def server_write(context, message):
    message = message.replace("|", chr(31)).replace("+", chr(30))
    context.server.write(message)


@then(u'the server has {num_connections} active connections')
def server_num_connections(context, num_connections):
    connections_count = len(context.server.connections)
    assert connections_count == int(num_connections), (str(connections_count) +
                                                       " active connections.")


@then(u'the last message the server recieved is "{message}"')
def server_last_message(context, message):
    message = message.replace("|", chr(31)).replace("+", chr(30))
    parsed_message = message_parser.parse(message, context.client)[0]
    assert len(context.server.received_messages) > 0, 'No messages received.'
    last_message = message_parser.parse(context.server.received_messages[-1],
                                        context.client)[0]
    assert last_message == parsed_message, '{0} is not {1}'.format(
        last_message, parsed_message)


@then(u'the clients connection state is "{state}"')
def client_connection_state(context, state):
    assert context.client.connection_state == state


@then(u'the last login was successful')
@testing.gen_test
def client_last_login(context):
    login_info = yield context.login_future
    assert login_info['success']


@then(u'the last login failed with error "{error}" and message "{message}"')
@testing.gen_test
def client_last_login_failed(context, error, message):
    login_info = yield context.login_future
    assert not login_info['success'], "Successful login"
    assert (login_info['error'] == error,
            "Expected: " + error +
            "; Got: " + login_info['error'])
    assert (login_info['message'] == message,
            "Expected: " + message +
            "; Got: " + login_info['message'])


@then(u'the server has received {num_messages} messages')
def server_num_messages(context, num_messages):
    received = len(context.server.received_messages)
    expected = int(num_messages.encode())
    assert received == expected,  ("The server has received " + str(received) +
                                   " messages, not " + str(expected))


@then(u'the client throws a "{event}" error with message "{message}"')
def client_error(context, event, message):
    matching_errors = [e for e in context.client_errors if
                       e['event'] == event and
                       e['message'] == message]

    assert len(matching_errors) > 0, context.client_errors
