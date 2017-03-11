from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import client
from deepstreampy import rpc
from deepstreampy import constants
from tests.util import msg

from tornado import testing
import functools
import sys

if sys.version_info[0] < 3:
    import mock
else:
    from unittest import mock

URL = "ws://localhost:7777/deepstream"


class RPCHandlerTest(testing.AsyncTestCase):

    def setUp(self):
        super(RPCHandlerTest, self).setUp()
        options = {'rpcResponseTimeout': 0.01,
                   'rpcAckTimeout': 0.01,
                   'subscriptionTimeout': 0.01}
        self.client = client.Client(URL, **options)
        self.handler = mock.Mock()
        self.handler.stream.closed = mock.Mock(return_value=False)
        self.client._connection._state = constants.connection_state.OPEN
        self.client._connection._websocket_handler = self.handler
        self.connection = self.client._connection
        self.io_loop = self.connection._io_loop
        self.rpc_calls = 0
        self.client_errors = []
        self.client.on('error', self._error_callback)

    def _error_callback(self, *args):
        self.client_errors.append(args)
        self.stop()

    def _add_two_callback(self, data, response):
        self.rpc_calls += 1
        result = data['numA'] + data['numB']
        if 'sync' in data and data['sync']:
            response.send(result)
            self.stop()
        else:
            self.connection._io_loop.call_later(
                0.5, functools.partial(response.send, result))
            self.connection._io_loop.call_later(0.6, self.stop)

    def test_handle_rpc_providers(self):
        # RPCHandler is created
        rpc_handler = self.client.rpc
        self.assertTrue(isinstance(self.client.rpc, rpc.RPCHandler))

        # Register a provider for addTwo RPC
        rpc_handler.provide('addTwo', self._add_two_callback)
        self.handler.write_message.assert_called_once_with(msg('P|S|addTwo+'))
        self.assertEquals(self.rpc_calls, 0)

        # Timeout error emitted if no ack message received on time
        self.wait()
        expected_error = ('No ACK message received in time for addTwo',
                          'ACK_TIMEOUT',
                          'P')
        self.assertTrue(expected_error in self.client_errors)

        # Reply to a sync RPC request
        rpc_handler._handle({'topic': 'RPC',
                             'action': 'REQ',
                             'data': ['addTwo',
                                      '678',
                                      'O{"numA":2,"numB":3,"sync":true}']})
        self.wait()
        self.handler.write_message.assert_called_with(
            msg('P|RES|addTwo|678|N5+'))

        # Reply to an async RPC request
        rpc_handler._handle({'topic': 'RPC',
                             'action': 'REQ',
                             'data': ['addTwo', '123', 'O{"numA":7,"numB":3}']})

        self.connection._io_loop.call_later(0.1, self.stop)
        self.wait()
        self.handler.write_message.assert_called_with(
            msg('P|A|REQ|addTwo|123+'))

        self.wait()
        self.handler.write_message.assert_called_with(
            msg('P|RES|addTwo|123|N10+'))

        # Send rejection if no provider exists
        rpc_handler._handle({'topic': 'RPC',
                             'action': 'REQ',
                             'data': ['doesNotExist', '432',
                                      'O{"numA":7,"numB":3}']})
        self.handler.write_message.assert_called_with(
            msg('P|REJ|doesNotExist|432+'))

        # Deregister provider for the addTwo RPC
        rpc_handler.unprovide('addTwo')
        self.handler.write_message.assert_called_with(msg('P|US|addTwo+'))

        # Timeout emitted after no ACK message received for the unprovide
        self.client_errors = []
        self.connection._io_loop.call_later(3.5, self.stop)
        self.wait()
        expected_error = ('No ACK message received in time for addTwo',
                          'ACK_TIMEOUT',
                          'P')
        self.assertTrue(expected_error in self.client_errors)

        # Reject call to deregistered provider
        rpc_handler._handle({
            'topc': 'RPC',
            'action': 'REQ',
            'data': ['addTwo', '434', 'O{"numA":2,"numB":7, "sync": true}']})

        self.handler.write_message.assert_called_with(msg('P|REJ|addTwo|434+'))

    def test_make_rpcs(self):
        # RPCHandler is created
        rpc_handler = self.client.rpc
        self.assertTrue(isinstance(self.client.rpc, rpc.RPCHandler))

        self.client.get_uid = mock.Mock(return_value='1')

        # Make a successful RPC for addTwo
        rpc_callback = mock.Mock()
        rpc_handler.make('addTwo', {'numA': 3, 'numB': 8}, rpc_callback)
        self.assertTrue(self.handler.write_message.call_args[0][0] in
                        (msg('P|REQ|addTwo|1|O{"numA":3,"numB":8}+'),
                         msg('P|REQ|addTwo|1|O{"numB":8,"numA":3}+')))

        rpc_handler._handle({'topic': 'RPC',
                             'action': 'RES',
                             'data': ['addTwo', u'1', 'N11']})

        rpc_callback.assert_called_with(None, 11)

        # Make RPC for addTwo but receive an error
        self.assertTrue(self.handler.write_message.call_args[0][0] in
                        (msg('P|REQ|addTwo|1|O{"numA":3,"numB":8}+'),
                         msg('P|REQ|addTwo|1|O{"numB":8,"numA":3}+')))

        rpc_handler.make('addTwo', {'numA': 3, 'numB': 8}, rpc_callback)
        rpc_handler._handle({'topic': 'RPC',
                             'action': 'E',
                             'data': ['NO_PROVIDER', 'addTwo', '1']})
        rpc_callback.assert_called_with('NO_PROVIDER', None)
        rpc_callback.reset_mock()

        # Make RPC for addTwo but receive no ack in time
        rpc_handler.make('addTwo', {'numA': 3, 'numB': 8}, rpc_callback)
        self.assertTrue(self.handler.write_message.call_args[0][0] in
                        (msg('P|REQ|addTwo|1|O{"numA":3,"numB":8}+'),
                         msg('P|REQ|addTwo|1|O{"numB":8,"numA":3}+')))

        self.connection._io_loop.call_later(2, self.stop)
        self.wait()
        rpc_callback.assert_called_with('ACK_TIMEOUT', None)


class RPCResponseTest(testing.AsyncTestCase):

    def setUp(self):
        super(RPCResponseTest, self).setUp()
        options = {'rpcResponseTimeout': 0.01, 'rpcAckTimeout': 0.01}
        self.client = client.Client(URL, **options)
        self.handler = mock.Mock()
        self.handler.stream.closed = mock.Mock(return_value=False)
        self.client._connection._state = constants.connection_state.OPEN
        self.client._connection._websocket_handler = self.handler
        self.connection = self.client._connection
        self.connection._io_loop = self.io_loop
        self.client_errors = []
        self.client.on('error', self._error_callback)

    def _error_callback(self, *args):
        self.client_errors.append(args)
        self.stop()

    def test_send(self):
        # Send auto ACK
        response = rpc.RPCResponse(self.connection, 'addTwo', '123')
        self.io_loop.call_later(1, self.stop)
        self.wait()
        self.handler.write_message.assert_called_with(
            msg('P|A|REQ|addTwo|123+'))

        # Send response
        response.send(14)
        self.handler.write_message.assert_called_with(
            msg('P|RES|addTwo|123|N14+'))

    @testing.gen_test
    def test_send_no_autoack(self):
        # Create response but disable autoack
        response = rpc.RPCResponse(self.connection, 'addTwo', '123')
        response.auto_ack = False
        self.handler.write_message.assert_not_called()

        # Send the ack
        response.ack()
        self.handler.write_message.assert_called_with(
            msg('P|A|REQ|addTwo|123+'))

        # Do not send multiple ack messages
        self.handler.reset_mock()
        response.ack()
        self.handler.write_message.assert_not_called()

    def test_reject(self):
        response = rpc.RPCResponse(self.connection, 'addTwo', '123')
        response.reject()
        self.handler.write_message.assert_called_with(
            msg('P|REJ|addTwo|123+'))
        self.assertRaises(ValueError, functools.partial(response.send, 'abc'))

    def test_error(self):
        response = rpc.RPCResponse(self.connection, 'addTwo', '123')
        response.error('Error message')
        self.handler.write_message.assert_called_with(
            msg('P|E|Error message|addTwo|123+'))
        self.assertRaises(ValueError, functools.partial(response.send, 'abc'))
