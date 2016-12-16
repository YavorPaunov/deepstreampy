from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from tornado import ioloop


def before_scenario(context, scenario):
    context.server = None
    context.client = None
    context.client_errors = []
    context.io_loop = ioloop.IOLoop.current()
    context.event_callbacks = {}
    context.listen_callback = None
    context.rpc_response = None


def after_scenario(context, scenario):
    context.server = None
    context.client = None
    context.login_future = None
    context.event_callbacks = {}
    context.listen_callback = None
