from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from tornado import ioloop, web, websocket, httpserver, concurrent

from collections import defaultdict
import mock


class DeepstreamHandler(websocket.WebSocketHandler):

    connections = defaultdict(set)
    received_messages = defaultdict(list)
    sent_messages = defaultdict(list)
    callbacks = defaultdict(mock.Mock)

    def open(self):
        self._messages = []
        DeepstreamHandler.connections[self.request.path].add(self)
        self._msg_future = None
        self._close_future = None

    def on_message(self, message):
        DeepstreamHandler.received_messages[self.request.path].append(message)
        if self._msg_future:
            self._msg_future.set_result(message)

    def write_message(self, message):
        DeepstreamHandler.sent_messages[self.request.path].append(message)
        super(DeepstreamHandler, self).write_message(message)

    def on_close(self):
        DeepstreamHandler.connections[self.request.path].discard(self)
        if self._close_future:
            self._close_future.set_result(True)

    def message_future(self):
        self._msg_future = concurrent.Future()
        return self._msg_future

    def close_future(self):
        self._close_future = concurrent.Future()
        return self._close_future


def _connections(request_path):
    return DeepstreamHandler.connections[request_path]


def _sent_messages(request_path):
    return DeepstreamHandler.sent_messages[request_path]


def _received_messages(request_path):
    return DeepstreamHandler.received_messages[request_path]


def _num_connection(request_path):
    return len(_connections(request_path))


def _create_server(port, path):
    application = web.Application([
        (path, DeepstreamHandler),
    ])
    server = httpserver.HTTPServer(application)
    server.listen(port)
    return server


def after_step(context, step):
    context.io_loop.call_later(0.025, context.io_loop.stop)
    context.io_loop.start()


def before_scenario(context, scenario):
    if ioloop.IOLoop.initialized():
        context.io_loop = ioloop.IOLoop.current()
    else:
        context.io_loop = ioloop.IOLoop(make_current=True)

    context.server = None
    context.other_server = None
    DeepstreamHandler.connections.clear()
    DeepstreamHandler.received_messages.clear()
    DeepstreamHandler.sent_messages.clear()
    DeepstreamHandler.callbacks.clear()

    context.create_server = _create_server
    context.num_connections = _num_connection
    context.connections = _connections
    context.sent_messages = _sent_messages
    context.received_messages = _received_messages

    context.client = None
    context.client_errors = []
    context.event_callbacks = {}
    context.has_callbacks = {}
    context.snapshot_callbacks = {}
    context.subscribe_callback = None
    context.presence_callback = None
    context.presence_query_callback = None
    context.rpc_provide_callback = None
    context.rpc_request_callback = None
    context.listen_callback = None
    context.rpc_response = None
    context.records = {}
    context.write_acknowledge = None
    context.login_future = None


def after_scenario(context, scenario):
    context.io_loop.clear_current()
    context.io_loop.close(all_fds=True)
