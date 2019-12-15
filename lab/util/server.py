from multiprocessing import Process, Queue
from lab.util import message
from lab.util import sockets
from json.decoder import JSONDecodeError


class ServerProcess:
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


class Server:
    def __init__(self):
        # separate method to avoid type casting
        self.re_init()

    def re_init(self, *args):
        self.message_interface = {}

        # Create queue
        self.server_queue = Queue()

        # Start server with queue
        self.server = Process(target=ServerProcess, args=(self.server_queue,))
        self.server.start()

        # Wait for server to send its hostname and port
        self.hostname, self.port = self.server_queue.get()

    def handle_queue(self):
        while self.message_in_queue():
            try:
                status, *args = self.get_message_from_queue()
                assert status in self.message_interface.keys(), \
                    f'Unknown status {status}'
                self.message_interface[status](*args)
            except JSONDecodeError:
                continue

    def get_message_from_queue(self) -> [str]:
        """
        :return: List of the elements of the data in the queue
        """
        return message.read(self.server_queue.get())

    def message_in_queue(self) -> bool:
        """
        :return: Boolean whether there are any messages in the queue
        """

        return not self.server_queue.empty()
