from tornado import tcpserver

class FakeServer(tcpserver.TCPServer):

    def __init__(self):
        super(FakeServer, self).__init__()
        self.queued_messages = list()
        self.received_messages = list()
        self.stream = None
        self.connections = []

    def write(self, message):
        self.queued_messages.append(message)
        self.stream.write(message.encode())

    def read(self, data):
        self.received_messages.append(data.decode())

    def handle_stream(self, stream, address):
        self.connections.append(stream)
        self.stream = stream
        self.stream.read_until_close(None, self.read)

    def reset_message_count(self):
        self.received_messages = list()
