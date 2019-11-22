from multiprocessing import Process, Queue
import time

from lab.util import sockets, message
from lab.util.meta_data import MetaData
from lab.util.server import Server


class Node:
    def __init__(self, worker_id: int, master_host: str, master_port: int):
        self.worker_id = worker_id
        self.master_host = master_host
        self.master_port = master_port

    def send_message_to_master(self, message_to_send: bytes):
        """
        Sends a message to the master
        """
        sockets.send_message(
            self.master_host, self.master_port, message_to_send)


class HearbeatDaemon(Node):
    def __init__(self, worker_id: int, master_host: str, master_port: int,
                 wait_time: float):
        super().__init__(worker_id, master_host, master_port)
        # give master time to init
        time.sleep(0.1)
        while True:
            self.send_message_to_master(message.write_alive(self.worker_id))
            time.sleep(wait_time)


class WorkerInterface(Node):
    """ Actually an abstract base class
    """

    def __init__(self, worker_id: int, master_host: str, master_port: int,
                 graph_path: str):
        super().__init__(worker_id, master_host, master_port)
        self.graph = graph_path

        # Create queue
        self.server_queue = Queue()

        # Start server with queue
        server = Process(target=Server, args=(self.server_queue,))
        server.start()
        self.init_hearbeat_daemon(wait_time=1)

        # Wait for server to send its hostname and port
        self.hostname, self.port = self.server_queue.get()

        # Register self at master
        self.register()

        # Wait for the meta data of the other workers
        self.meta_data_of_all_workers = self.receive_meta_data()

        self.run()

    def run(self):
        raise NotImplementedError()

    def get_message_from_queue(self) -> [str]:
        """
        :return: List of the elements of the data in the queue
        """
        return message.read(self.server_queue.get())

    def receive_meta_data(self):
        status, all_meta_data = self.get_message_from_queue()

        return [
            MetaData(
                worker_id=meta_data['worker_id'],
                min_vertex=meta_data['min_vertex'],
                max_vertex=meta_data['max_vertex'],
                host=meta_data['host'],
                port=meta_data['port']
            )
            for meta_data in all_meta_data
        ]

    def register(self):
        """
        Sends a REGISTER request to the master
        """

        self.send_message_to_master(message.write_register(
            self.worker_id, self.hostname, self.port))

    def init_hearbeat_daemon(self, wait_time=1):
        self.hearbeat_daemon = Process(target=HearbeatDaemon, args=(
            self.worker_id, self.master_host, self.master_port, wait_time))
        self.hearbeat_daemon.start()
