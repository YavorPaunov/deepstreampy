from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import connect
from deepstreampy.constants import connection_state
from deepstreampy.message import message_parser

from behave import given, when, then
from tornado import testing

import json
import sys

if sys.version_info[0] < 3:
    import mock
else:
    from unittest import mock

FIRST_SERVER_URL = "ws://localhost:7777/deepstream"
SECOND_SERVER_URL = "ws://localhost:8888/deepstream2"


@given(u'the test server is ready')
def server_ready(context):
    if not context.server:
        context.server = context.create_server(7777, '/deepstream')


@given(u'the second test server is ready')
def second_server_ready(context):
    if not context.other_server:
        context.other_server = context.create_server(8888, '/deepstream2')


@given(u'the client is initialised')
@testing.gen_test
def client_init(context):
    context.client = yield connect(FIRST_SERVER_URL,
                                   subscriptionTimeout=0.1,
                                   recordReadAckTimeout=0.2,
                                   recordReadTimeout=0.26,
                                   recordDeleteTimeout=0.1,
                                   rpcResponseTimeout=0.2)
    context.client.get_uid = mock.Mock(return_value='<UID>')

    def error_callback(message, event, t):
        context.client_errors.append(dict(message=message,
                                          event=event,
                                          topic=t))

    context.subscribe_callback = mock.Mock()
    context.client.on('error', error_callback)


@given(u'the client is initialised with a small heartbeat interval')
@testing.gen_test
def client_init_small_heartbeat(context):
    context.client = yield connect(FIRST_SERVER_URL,
                                   subscriptionTimeout=0.1,
                                   recordReadAckTimeout=0.2,
                                   recordReadTimeout=0.26,
                                   recordDeleteTimeout=0.1,
                                   rpcResponseTimeout=0.2,
                                   heartbeatInterval=0.5)
    context.client.get_uid = mock.Mock(return_value='<UID>')

    def error_callback(message, event, t):
        context.client_errors.append(dict(message=message,
                                          event=event,
                                          topic=t))

    context.subscribe_callback = mock.Mock()
    context.client.on('error', error_callback)


@given(u'the server resets its message count')
def server_reset_count(context):
    del context.received_messages('/deepstream')[:]


@given(u'the client logs in with username "{username}" and password '
       '"{password}"')
@when(u'the client logs in with username "{username}" and password '
      '"{password}"')
def client_login(context, username, password):
    auth_data = dict(username=username, password=password)
    context.login_future = context.client.login(auth_data)


@given(u'the server sends the message {message}')
@when(u'the server sends the message {message}')
@testing.gen_test
def server_sends_message(context, message):
    message = message.replace("<SECOND_SERVER_URL>", SECOND_SERVER_URL)
    message = message.replace("|", chr(31)).replace("+", chr(30))

    if (context.client and
            context.client._connection._url == SECOND_SERVER_URL):
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
    expected = expected.replace("<FIRST_SERVER_URL>", FIRST_SERVER_URL)
    parsed_expected = message_parser.parse(expected, context.client)[0]

    if context.client._connection._url == FIRST_SERVER_URL:
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
    actual = len(context.received_messages('/deepstream'))
    assert actual == expected,  (
        "The server has received " + str(actual) +
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
    record = yield context.client.record.get_record(record_name)
    context.records[record_name] = record


@then(u'the client record "{record_name}" data is {data}')
@testing.gen_test
def record_data(context, record_name, data):
    record = yield context.client.record.get_record(record_name)
    actual_data = record.get()
    data = json.loads(data)
    assert actual_data == data, "Actual data is {0}, not {1}".format(
        actual_data, data)


@when(u'the client sets the record "{record_name}" "{path}" to "{value}"')
@testing.gen_test
def set_record_path(context, record_name, value, path):
    record = yield context.client.record.get_record(record_name)
    record.set(value, path, context.write_acknowledge)


@when(u'the client sets the record "{record_name}" to {value}')
@testing.gen_test
def set_record(context, record_name, value):
    record = yield context.client.record.get_record(record_name)
    record.set(json.loads(value), callback=context.write_acknowledge)


@when(u'the client discards the record named "{record_name}"')
@testing.gen_test
def discard_record(context, record_name):
    record = yield context.client.record.get_record(record_name)
    yield record.discard()


@given(u'the client deletes the record named "{record_name}"')
@when(u'the client deletes the record named "{record_name}"')
@testing.gen_test
def delete_record(context, record_name):
    record = yield context.client.record.get_record(record_name)
    yield record.delete()


@given(u'the client checks if the server has the record "{record_name}"')
@testing.gen_test
def record_check_has(context, record_name):
    callback = mock.Mock()
    yield context.client.record.has(record_name, callback)
    context.has_callbacks[record_name] = callback


@then(u'the client is told the record "{record_name}" exists')
def record_exists(context, record_name):
    callback = context.has_callbacks[record_name]
    callback.assert_called_with(None, True)


@then(u'the client is told the record "{record_name}" doesn\'t exist')
def record_does_not_exist(context, record_name):
    callback = context.has_callbacks[record_name]
    callback.assert_called_with(None, False)


@then(u'the server didn\'t receive the message {message}')
def step_impl(context, message):
    expected = message.replace("|", chr(31)).replace("+", chr(30))
    all_messages = context.received_messages('/deepstream')
    assert expected not in all_messages, all_messages


@when(u'the client listens to a record matching "{pattern}"')
@testing.gen_test
def listen(context, pattern):
    context.listen_callback = mock.Mock()
    yield context.client.record.listen(pattern, context.listen_callback)


@given(u'two seconds later')
@when(u'two seconds later')
def two_seconds_later(context):
    context.io_loop.call_later(2, context.io_loop.stop)
    context.io_loop.start()


@when(u'some time passes')
@given(u'some time passes')
def sleep(context):
    context.io_loop.call_later(0.2, context.io_loop.stop)
    context.io_loop.start()


@when(u'the client unlistens to a record matching "{pattern}"')
@testing.gen_test
def unlisten(context, pattern):
    yield context.client.record.unlisten(pattern)


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
@testing.gen_test
def client_subscribes(context, record_name):
    record = yield context.client.record.get_record(record_name)
    context.subscribe_callback = mock.Mock()
    record.subscribe(context.subscribe_callback)


@given(u'the client unsubscribes to the entire record "{record_name}" changes')
@testing.gen_test
def client_unsubscribes(context, record_name):
    record = yield context.client.record.get_record(record_name)
    record.unsubscribe(context.subscribe_callback)
    context.subscribe_callback.reset_mock()


@when(u'the client subscribes to "{path}" for the record "{record_name}"')
@testing.gen_test
def record_path_subscribe(context, path, record_name):
    record = yield context.client.record.get_record(record_name)
    context.subscribe_callback = mock.Mock()
    record.subscribe(context.subscribe_callback, path)


@given(u'the client unsubscribes to "{path}" for the record "{record_name}"')
@testing.gen_test
def record_unsubscribe(context, path, record_name):
    record = yield context.client.record.get_record(record_name)
    record.unsubscribe(context.subscribe_callback, path)
    context.subscribe_callback.reset_mock()


@then(u'the client will be notified of the record change')
@then(u'the client will be notified of the partial record change')
@then(u'the client will be notified of the second record change')
def client_notified_record(context):
    assert context.subscribe_callback.called
    context.subscribe_callback.reset_mock()


@then(u'the client will not be notified of the record change')
def client_not_notified(context):
    context.subscribe_callback.assert_not_called()


@given(u'the client requests a snapshot for the record "{record_name}"')
def record_request_snapshot(context, record_name):
    callback = mock.Mock()
    context.client.record.snapshot(record_name, callback)
    context.snapshot_callbacks[record_name] = callback


@then(u'the client has no response for the snapshot of record "{record_name}"')
def record_no_snapshot(context, record_name):
    callback = context.snapshot_callbacks[record_name]
    callback.assert_not_called()


@then(u'the client is told the record "{record_name}" encountered an error '
      'retrieving snapshot')
def record_snapshot_error(context, record_name):
    callback = context.snapshot_callbacks[record_name]
    error = callback.call_args[0][0]
    assert error is not None


@then(u'the client is provided the snapshot for record "{record_name}" with '
      'data "{data}"')
def record_snapshot(context, record_name, data):
    callback = context.snapshot_callbacks[record_name]
    callback.assert_called_with(None, json.loads(data))


@given(u'the client subscribes to an event named "{event_name}"')
@when(u'the client subscribes to an event named "{event_name}"')
@testing.gen_test
def event_subscribe(context, event_name):
    context.event_callbacks[event_name] = mock.Mock()
    yield context.client.event.subscribe(
        event_name, context.event_callbacks[event_name])


@given(u'the client unsubscribes from an event named "{event_name}"')
@when(u'the client unsubscribes from an event named "{event_name}"')
@testing.gen_test
def event_unsubscribe(context, event_name):
    yield context.client.event.unsubscribe(
        event_name, context.event_callbacks[event_name])
    del context.event_callbacks[event_name]


@when(u'the client listens to events matching "{event_pattern}"')
@testing.gen_test
def event_listen(context, event_pattern):
    context.listen_callback = mock.Mock()
    yield context.client.event.listen(event_pattern, context.listen_callback)


@when(u'the client publishes an event named "{event_name}" with data '
      '"{event_data}"')
def event_publish(context, event_name, event_data):
    context.client.event.emit(event_name, event_data)


@when(u'the client unlistens to events matching "{event_pattern}"')
@testing.gen_test
def event_unlisten(context, event_pattern):
    yield context.client.event.unlisten(event_pattern)


@then(u'the client will be notified of new event match "{event_match}"')
def event_match_new(context, event_match):
    found_match = False
    for args in context.listen_callback.call_args_list:
        found_match = (event_match, True) == args[0][:2]
    assert found_match


@then(u'the client will be notified of event match removal "{event_match}"')
def event_match_removal(context, event_match):
    context.listen_callback.assert_called_with(event_match, False)


@then(u'the client received the event "{event_name}" with data "{event_data}"')
def event_received(context, event_name, event_data):
    callback = context.event_callbacks[event_name]
    callback.assert_called_with(event_data)


@then(u'the server did not recieve any messages')
def server_no_message(context):
    assert len(context.received_messages('/deepstream')) == 0


@when(u'the client provides a RPC called "{rpc_name}"')
@testing.gen_test
def rpc_provide(context, rpc_name):
    context.rpc_provide_callback = mock.Mock()
    yield context.client.rpc.provide(rpc_name, context.rpc_provide_callback)


@then(u'the client recieves a request for a RPC called "{rpc_name}" with data '
      '"{data}"')
def rpc_request(context, rpc_name, data):
    if context.client._connection.state == connection_state.RECONNECTING:
        context.io_loop.call_later(3, context.io_loop.stop)
        context.io_loop.start()
    context.rpc_response = context.rpc_provide_callback.call_args[0][1]
    context.rpc_provide_callback.reset_mock()


@when(u'the client responds to the RPC "{rpc_name}" with data "{data}"')
@testing.gen_test
def rpc_respond_data(context, rpc_name, data):
    if not context.rpc_response:
        context.rpc_response = context.rpc_provide_callback.call_args[0][1]

    yield context.rpc_response.send(data)

    msg_future = next(iter(
        context.connections('/deepstream'))).message_future()
    yield msg_future


@when(u'the client responds to the RPC "{rpc_name}" with the error "{error}"')
@testing.gen_test
def rpc_respond_error(context, rpc_name, error):
    if not context.rpc_response:
        context.rpc_response = context.rpc_provide_callback.call_args[0][1]

    yield context.rpc_response.error(error)


@when(u'the client rejects the RPC "{rpc_name}"')
@testing.gen_test
def rpc_reject(context, rpc_name):
    if not context.rpc_response:
        context.rpc_response = context.rpc_provide_callback.call_args[0][1]

    yield context.rpc_response.reject()


@when(u'the client stops providing a RPC called "{rpc_name}"')
@testing.gen_test
def rpc_unprovide(context, rpc_name):
    yield context.client.rpc.unprovide(rpc_name)


@when(u'the client requests RPC "{rpc_name}" with data "{data}"')
@testing.gen_test
def rpc_client_request(context, rpc_name, data):
    context.rpc_request_callback = mock.Mock()
    yield context.client.rpc.make(rpc_name, data, context.rpc_request_callback)


@then(u'the client recieves a successful RPC callback for "{rpc_name}" with '
      'data "{data}"')
def rpc_callback(context, rpc_name, data):
    context.rpc_request_callback.assert_called_with(None, data)


@then(u'the client recieves an error RPC callback for "{rpc_name}" with the '
      'message "{error}"')
def rpc_callback_error(context, rpc_name, error):
    found_error = False
    for err in context.rpc_request_callback.call_args_list:
        if err[0][0] == error:
            found_error = True

    assert found_error, "Error {0} not thrown".format(error)


@given(u'the client subscribes to presence events')
@testing.gen_test
def presence_subscribe(context):
    context.presence_callback = mock.Mock()
    yield context.client.presence.subscribe(context.presence_callback)


@given(u'the client queries for connected clients')
@testing.gen_test
def presence_query(context):
    context.presence_query_callback = mock.Mock()
    future = context.client.presence.get_all(context.presence_query_callback)
    if not context.client._connection._websocket_handler.stream.closed():
        yield future


@then(u'the client is notified that no clients are connected')
def presence_notify_no_clients(context):
    context.presence_query_callback.assert_called_with([])


@then(u'the client is notified that clients "{client_names}" are connected')
def presence_notify_clients(context, client_names):
    context.presence_query_callback.assert_called_with(client_names.split(','))


@then(u'the client is notified that client "{client_name}" logged in')
def presence_notify_log_in(context, client_name):
    context.presence_callback.assert_called_with(client_name, True)


@then(u'the client is not notified that client "{client_name}" logged in')
def presence_no_notify_log_in(context, client_name):
    context.presence_callback.assert_not_called()


@then(u'the client is notified that client "{client_name}" logged out')
def presence_notify_log_out(context, client_name):
    context.presence_callback.assert_called_with(client_name, False)


@when(u'the client unsubscribes to presence events')
@given(u'the client unsubscribes to presence events')
@testing.gen_test
def presence_unsubscribe(context):
    yield context.client.presence.unsubscribe(context.presence_callback)
    context.presence_callback = mock.Mock()


@given(u'the client requires write acknowledgement on record "{record_name}"')
def record_write_acknowledge(context, record_name):
    context.write_acknowledge = mock.Mock()


@then(u'the client is notified that the record "{record_name}" was written '
      'without error')
@testing.gen_test
def record_write_acknowledge_success(context, record_name):
    if context.client._connection.state != connection_state.OPEN:
        yield context.client.connect()
    context.write_acknowledge.assert_called_with(None)


@then(u'the client is notified that the record "{record_name}" was written '
      'with error "{error}"')
def record_write_acknowledge_error(context, record_name, error):
    context.write_acknowledge.assert_called_with(error)


@then(u'the client is on the second server')
def client_on_second_server(context):
    assert context.client._connection._url == SECOND_SERVER_URL
