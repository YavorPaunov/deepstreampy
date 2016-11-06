from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import client
from deepstreampy.message import message_parser

from behave import *
from tornado import testing

import json
import time
import sys

if sys.version_info.major < 3:
    import mock
else:
    from unittest import mock

PORT = 7777
HOST = "localhost"


class Server:

    def __init__(self):
        self.recieved_messages = []
        self._client = None
        self._connection = None
        self.iostream = None
        self._num_connections = 0

    def add_client(self, value):
        self._num_connections += 1
        self._client = value
        self._connection = self._client._connection

    def reset_messages(self):
        if self._connection:
            self._connection._stream.reset_mock()

    def get_last_message(self):
        return self._connection._stream.write.call_args[0][0]

    def get_all_messages(self):
        stream = self._connection._stream
        all_messages = [msg[0][0] for msg in stream.write.call_args_list]
        return all_messages

    def assert_num_messages(self, expected_num):
        if self._connection:
            actual = self._connection._stream.write.call_count
            assert actual == expected_num, actual
        else:
            assert False


@given(u'the test server is ready')
def server_ready(context):
    # server = FakeServer()
    # server.listen(PORT)
    # context.server = server

    context.server = Server()


@given(u'the client is initialised')
def client_init(context):
    context.client = client.Client(HOST, PORT)

    def error_callback(message, event, t):
        context.client_errors.append(dict(message=message,
                                          event=event,
                                          topic=t))

    context.client_stream = mock.Mock()
    dummy_future = mock.Mock()
    dummy_future.result.return_value = context.client_stream
    dummy_future.exception.return_value = None

    context.client.on('error', error_callback)
    context.client._connection._on_open(dummy_future)
    context.server.add_client(context.client)
    context.subscribe_callback = mock.Mock()


@given(u'the server resets its message count')
def server_reset_count(context):
    # context.server.reset_message_count()
    context.server.reset_messages()


@given(u'the client logs in with username "{username}" and password '
       '"{password}"')
@when(u'the client logs in with username "{username}" and password '
      '"{password}"')
@testing.gen_test
def client_login(context, username, password):
    auth_data = dict(username=username, password=password)
    # server_future = context.server.await_message()
    context.login_future = context.client.login(auth_data)


@given(u'the server sends the message {message}')
@when(u'the server sends the message {message}')
@testing.gen_test
def server_sends_message(context, message):
    message = message.replace("|", chr(31)).replace("+", chr(30))
    context.client._connection._on_data(message)


@then(u'the server has {num_connections} active connections')
def server_num_connections(context, num_connections):
    connections_count = context.server._num_connections
    assert connections_count == int(num_connections), (str(connections_count) +
                                                       " active connections.")


@then(u'the last message the server recieved is {message}')
def server_last_message(context, message):
    expected = message.replace("|", chr(31)).replace("+", chr(30)).encode()
    actual = context.server.get_last_message()
    assert expected == actual, ("Got: {1}, expected: {0}".format(expected,
                                                                 actual))


@then(u'the clients connection state is "{state}"')
def client_connection_state(context, state):
    assert context.client.connection_state == state, (
        "Actual state is {0}".format(context.client.connection_state))


@then(u'the last login was successful')
def client_last_login(context):
    login_info = yield context.login_future
    assert login_info['success']


@then(u'the last login failed with error message "{message}"')
def client_last_login_failed(context, message):
    login_info = yield context.login_future
    assert not login_info['success'], "Successful login"
    assert login_info['message'] == message, (
        "Expected: " + message + "; Got: " + login_info['message'])


@then(u'the server has received {num_messages} messages')
def server_num_messages(context, num_messages):
    expected = int(num_messages.encode())
    context.server.assert_num_messages(expected)
    return
    received = len(context.server.received_messages)
    expected = int(num_messages.encode())
    assert received == expected.encode(),  (
        "The server has received " + str(received) +
        " messages, not " + str(expected))


@then(u'the client throws a "{event}" error with message "{message}"')
def client_error(context, event, message):
    matching_errors = [e for e in context.client_errors if
                       e['event'] == event and
                       e['message'] == message]
    assert len(matching_errors) > 0, context.client_errors


@given(u'the client creates a record named "{record_name}"')
@when(u'the client creates a record named "{record_name}"')
@testing.gen_test
def create_record(context, record_name):
    context.client.record.get_record(record_name)


@then(u'the client record "{record_name}" data is {data}')
def record_data(context, record_name, data):
    record = context.client.record.get_record(record_name)
    actual_data = record.get()
    data = json.loads(data)
    assert actual_data == data, "Actual data is {0}, not {1}".format(
        actual_data, data)


@when(u'the client sets the record "{record_name}" "{path}" to "{value}"')
@testing.gen_test
def set_record_path(context, record_name, value, path):
    record = context.client.record.get_record(record_name)
    record.set(value, path)


@when(u'the client sets the record "{record_name}" to {value}')
@testing.gen_test
def set_record(context, record_name, value):
    record = context.client.record.get_record(record_name)
    record.set(json.loads(value))


@when(u'the client discards the record named "{record_name}"')
def discard_record(context, record_name):
    record = context.client.record.get_record(record_name)
    record.discard()


@given(u'the client deletes the record named "{record_name}"')
@when(u'the client deletes the record named "{record_name}"')
def delete_record(context, record_name):
    record = context.client.record.get_record(record_name)
    record.delete()


@when(u'the client listens to a record matching "{pattern}"')
def listen(context, pattern):
    context.listen_callback = mock.Mock()
    context.client.record.listen(pattern, context.listen_callback)


@given(u'two seconds later')
def step_impl(context):
    def stop_io_loop():
        context.io_loop.stop()

    context.io_loop.call_later(2, stop_io_loop)
    context.io_loop.start()


@when(u'some time passes')
def sleep(context):
    def stop_io_loop():
        context.io_loop.stop()

    context.io_loop.call_later(1, stop_io_loop)
    context.io_loop.start()


@when(u'the client unlistens to a record matching "{pattern}"')
def unlisten(context, pattern):
    context.client.record.unlisten(pattern)


@then(u'the server received the message {message}')
def received_message(context, message):
    expected = message.replace("|", chr(31)).replace("+", chr(30)).encode()
    all_messages = context.server.get_all_messages()
    assert expected in all_messages, all_messages


@when(u'the connection to the server is lost')
def connection_lost(context):
    context.client._connection._on_close()


@when(u'the connection to the server is reestablished')
def connection_reestablished(context):
    dummy_future = mock.Mock()
    dummy_future.result.return_value = context.client_stream
    dummy_future.exception.return_value = None
    context.client._connection._on_open(dummy_future)


@then(u'the client will be notified of new record match "{match}"')
def client_notified(context, match):
    match_found = False
    for call_args in context.listen_callback.call_args_list:
        args = call_args[0]
        if args[0] == match and args[1]:
            match_found = True
    assert match_found


@then(u'the client will be notified of record match removal "{match}"')
def client_notified_of_removal(context, match):
    match_found = False
    for call_args in context.listen_callback.call_args_list:
        args = call_args[0]
        if args[0] == match and not args[1]:
            match_found = True
    assert match_found


@when(u'the client subscribes to the entire record "{record_name}" changes')
def client_subscribes(context, record_name):
    record = context.client.record.get_record(record_name)
    record.subscribe(context.subscribe_callback)


@given(u'the client unsubscribes to the entire record "{record_name}" changes')
def client_unsubscribes(context, record_name):
    record = context.client.record.get_record(record_name)
    record.unsubscribe(context.subscribe_callback)
    context.subscribe_callback.reset_mock()


@when(u'the client subscribes to "{path}" for the record "{record_name}"')
def step_impl(context, path, record_name):
    record = context.client.record.get_record(record_name)
    record.subscribe(context.subscribe_callback, path)


@given(u'the client unsubscribes to "{path}" for the record "{record_name}"')
def step_impl(context, path, record_name):
    record = context.client.record.get_record(record_name)
    record.unsubscribe(context.subscribe_callback, path)
    context.subscribe_callback.reset_mock()


@then(u'the client will be notified of the record change')
@then(u'the client will be notified of the partial record change')
@then(u'the client will be notified of the second record change')
def client_notified(context):
    assert context.subscribe_callback.called
    context.subscribe_callback.reset_mock()


@then(u'the client will not be notified of the record change')
def client_not_notified(context):
    assert not context.subscribe_callback.called
