from lab.util import command_line, message, sockets
from lab.util.file_io import read_in_chunks, write_chunk, get_start_vertex, get_first_line, get_last_line
from multiprocessing import Process, Queue
from lab.util.server import Server
from time import time, sleep
from lab.util.meta_data import MetaData


class Master:
    def __init__(self, n_workers: int, graph_path: str, worker_script: str, split_graph: bool):
        self.worker_script = worker_script
        self.n_workers = n_workers

        # Create queue
        self.server_queue = Queue()

        # Start server with queue
        server = Process(target=Server, args=(self.server_queue,))
        server.start()

        # Wait for server to send its hostname and port
        self.hostname, self.port = self.server_queue.get()

        # Split graph into sub graphs and send them to the workers
        self.sub_graph_paths = self.divide_graph(graph_path, split_graph)
        self.workers = self.create_workers()

        # Can be used to handle incoming messages from the server
        self.message_handler_interface = {
            message.ALIVE: self.handle_alive,
            message.REGISTER: self.handle_register
        }

        self.register_workers()
        self.send_meta_data_to_workers()

        # Run master until stopped
        try:
            self.run()
        except KeyboardInterrupt:
            self.terminate_workers()
            server.close()

    def divide_graph(self, graph_path: str, split_graph: bool) -> [str]:
        """
        Divides the graph into `number_of_workers` sub graphs and writes each chunk to a separate file

        :param graph_path: Path to the file containing the entire graph
        :param split_graph: Should the graph be split
        :return: List of paths to the created chunks
        """

        if split_graph:
            paths = []

            f = open(graph_path, "r")
            for worker_id, sub_graph in enumerate(read_in_chunks(f, self.n_workers)):
                paths.append(write_chunk(worker_id, sub_graph))
            f.close()
        else:
            # TODO do not duplicate data
            paths = [graph_path for _ in range(self.n_workers)]

        return paths

    def create_workers(self) -> dict:
        """
        Creates `self.n_workers` workers
        :return: Dictionary containing info about each worker
        """
        workers = {}

        for worker_id, sub_graph_path in enumerate(self.sub_graph_paths):
            workers[worker_id] = {
                'meta-data': MetaData(
                    worker_id=worker_id,
                    min_vertex=get_start_vertex(get_first_line(sub_graph_path)),
                    max_vertex=get_start_vertex(get_last_line(sub_graph_path))
                ),
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

        self.workers[worker_id]['last-alive'] = time()
        print(f"Worker {worker_id} is still alive")

    def handle_register(self, worker_id, host, port):
        """
        Handles the registration of a worker

        :param worker_id: Id of worker
        :param host: Host of worker
        :param port: Port of worker
        """

        self.workers[worker_id]['meta-data'].set_connection_info(host, port)
        self.workers[worker_id]['last-alive'] = time()
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
        return message.read(self.server_queue.get())

    def register_workers(self):
        for i in range(len(self.workers)):
            status, *args = self.get_message_from_queue()
            self.handle_register(*args)

    def send_meta_data_to_workers(self):
        all_meta_data = message.write_meta_data([worker['meta-data'].to_dict() for worker_id, worker in self.workers.items()])

        for worker_id, worker in self.workers.items():
            sockets.send_message(*worker['meta-data'].get_connection_info(), all_meta_data)

    def run(self):
        """
        Runs the master
        """

        while True:
            sleep(0.1)
            while self.message_in_queue():
                status, *args = self.get_message_from_queue()
                self.message_handler_interface[status](*args)
