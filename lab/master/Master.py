from lab.util import command_line, message
from lab.util.file_io import read_in_chunks, write_chunk
from multiprocessing import Process, Queue
from lab.util.server import Server
from datetime import datetime
from time import time, sleep


class Master:
    def __init__(self, n_workers: int, graph_path: str, worker_script: str):
        self.worker_script = worker_script
        self.n_workers = n_workers

        # Create queue
        self.server_queue = Queue()

        # Start server with queue
        server = Process(target=Server, args=(self.server_queue,))
        server.start()

        # Wait for server to send its hostname and port
        self.hostname, self.port = self.server_queue.get()

        # Split graph into subgraphs and send them to the workers
        self.sub_graph_paths = self.divide_graph(graph_path)
        self.workers = self.create_workers

        # Can be used to handle incoming messages from the server
        self.message_handler_interface = {
            message.ALIVE: self.handle_alive,
            message.REGISTER: self.handle_register
        }

        # Run master until stopped
        try:
            self.run()
        except KeyboardInterrupt:
            self.terminate_workers()
            server.close()

    def divide_graph(self, graph_path: str) -> [str]:
        """
        Divides the graph into `number_of_workers` sub graphs and writes each chunk to a separate file

        :param graph_path: Path to the file containing the entire graph
        :return: List of paths to the created chunks
        """
        paths = []

        f = open(graph_path, "r")
        for worker_id, sub_graph in enumerate(read_in_chunks(f, self.n_workers)):
            paths.append(write_chunk(worker_id, sub_graph))
        f.close()

        return paths

    @property
    def create_workers(self) -> dict:
        """
        Creates `self.n_workers` workers
        :return: Dictionary containing info about each worker
        """
        workers = {}

        for worker_id, sub_graph_path in enumerate(self.sub_graph_paths):
            workers[str(worker_id)] = {
                'host': None,
                'port': None,
                'last-alive': None,
                'process': command_line.setup_worker(
                    self.worker_script,
                    worker_id,
                    self.hostname,
                    self.port,
                    sub_graph_path
                )
            }

        return workers

    def terminate_workers(self):
        """
        Terminates the alive workers
        """

        for worker_id, worker in self.workers.items():
            if worker['process'] is not None:
                worker['process'].terminate()

    def handle_alive(self, worker_id):
        """
        Updates the last-alive value of the worker

        :param worker_id: Id of worker
        """

        self.workers[worker_id]['last-alive'] = datetime.now()
        print(f"Worker {worker_id} is still alive")

    def handle_register(self, worker_id, host, port):
        """
        Handles the registration of a worker

        :param worker_id: Id of worker
        :param host: Host of worker
        :param port: Port of worker
        """

        self.workers[worker_id]['host'] = host
        self.workers[worker_id]['port'] = port
        self.workers[worker_id]['last-alive'] = datetime.now()
        print(f"Registered worker {worker_id} on {host}:{port}")

    def message_in_queue(self) -> bool:
        """
        :return: Boolean whether there are any messages in the queue
        """

        return not self.server_queue.empty()

    def get_message_from_queue(self) -> [str]:
        """
        :return: List of the elements of the data in the queue
        """
        return self.server_queue.get().split(',')

    def run(self):
        """
        Runs the master
        """

        while True:
            sleep(0.1)
            while self.message_in_queue():
                status, *args = self.get_message_from_queue()
                self.message_handler_interface[status](*args)
