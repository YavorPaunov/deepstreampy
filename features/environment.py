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


def after_step(context, step):
    context.io_loop.call_later(0.05, context.io_loop.stop)
    context.io_loop.start()


def before_scenario(context, scenario):
    for conn in DeepstreamHandler.connections['/deepstream']:
        conn.close()
    for conn in DeepstreamHandler.connections['/deepstream2']:
        conn.close()
    DeepstreamHandler.connections.clear()
    DeepstreamHandler.received_messages.clear()
    DeepstreamHandler.sent_messages.clear()
    DeepstreamHandler.callbacks.clear()
    application = web.Application([
        ('/deepstream', DeepstreamHandler),
        ('/deepstream2', DeepstreamHandler)
    ])

    context.server = httpserver.HTTPServer(application)
    context.server.listen(7777)

    context.num_connections = (lambda request_path:
                               len(_connections(request_path)))
    context.connections = _connections
    context.sent_messages = _sent_messages
    context.received_messages = _received_messages

    context.client = None
    context.client_errors = []
    context.io_loop = ioloop.IOLoop.current()
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
    if context.client:
        context.client.close()
    context.server.stop()

    del context.server
    del context.client
