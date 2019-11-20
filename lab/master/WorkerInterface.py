from lab.util import sockets
from lab.util.message import REGISTER, ALIVE
import time
from multiprocessing import Process, Queue
from lab.util.server import Server


class WorkerInterface:
    """ Actually an abstract base class
    """

    def __init__(self, worker_id: int, master_host: str, master_port: int,
                 graph_path: str):
        self.worker_id = worker_id
        self.master_host = master_host
        self.master_port = master_port
        self.graph = graph_path

        # Create queue
        self.server_queue = Queue()

        # Start server with queue
        server = Process(target=Server, args=(self.server_queue,))
        server.start()

        # Wait for server to send its hostname and port
        self.hostname, self.port = self.server_queue.get()

        self.register()
        self.run()

    def run(self):
        raise NotImplementedError()

    def send_message_to_master(self, status: str, *args):
        """
        Sends a message to the master

        :param status: Status of the message (Should be one mentioned in lab.util.message)
        """

        sockets.send_message(self.master_host, self.master_port, status, *args)

    def register(self):
        """
        Sends a REGISTER request to the master
        """
        self.send_message_to_master(
            REGISTER, self.worker_id, self.hostname, self.port)
