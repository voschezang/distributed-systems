from multiprocessing import Queue
from lab.util import sockets


class Server:
    def __init__(self, queue: Queue):
        self.socket = sockets.bind("", 0)
        self.queue = queue
        queue.put([sockets.get_hostname(), sockets.get_port(self.socket)])
        self.listen()

    def put_message_in_queue(self, message):
        self.queue.put_nowait(message)

    def listen(self):
        while True:
            self.put_message_in_queue(sockets.get_message(self.socket))
