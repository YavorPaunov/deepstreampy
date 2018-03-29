"""
An example showing how to use remote procedure calls with deepstreampy.
"""
from __future__ import absolute_import, division, print_function, with_statement
from __future__ import unicode_literals

from deepstreampy import connect
from deepstreampy import rpc

from tornado import gen
from tornado import ioloop

import signal


def sq(x, response):
    """
    An RPC handler function that sends the square root of the input.
    """
    if isinstance(x, int):
        response.send(x * x)
    else:
        response.error("sq received unexpected input")


@gen.coroutine
def run():
    # First we set up the provider
    provider = yield connect("ws://localhost:6020/deepstream")
    yield provider.login({"username": "A"})
    provider.on('error', lambda msg, event, topic: print('a provider error occured'))

    yield provider.rpc.provide("sq", sq)

    # Then we set up the consumer
    consumer = yield connect("ws://localhost:6020/deepstream")
    yield consumer.login({"username": "B"})
    consumer.on('error', lambda msg, event, topic: print('a consumer error occured'))

    # Next we make an RPC call to the "sq" function
    result = yield consumer.rpc.make("sq", 2)
    print(result)
    # Output: 4

    # In case the provider ends up sending an error message, an exception
    # with that message will be raised when trying to get the result of the
    # call.
    try:
        yield consumer.rpc.make("sq", "2")
    except rpc.RPCException as e:
        print(e)
        # Output: sq received unexpected input


if __name__ == "__main__":
    run()
    signal.signal(signal.SIGINT, lambda _, __: ioloop.IOLoop.current().stop())
    ioloop.IOLoop.current().start()
