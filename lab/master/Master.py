from lab.util import command_line, message, sockets
from lab.util.file_io import read_in_chunks, write_chunk, get_start_vertex, get_first_line, get_last_line, read_as_reversed_edges, append_edge, get_number_of_lines, sort_file
from multiprocessing import Process, Queue
from lab.util.server import Server
from time import time, sleep
from lab.util.meta_data import MetaData, CombinedMetaData
from sys import stdout
from lab.util.distributed_graph import DistributedGraph


class Master:
    def __init__(self, n_workers: int, graph_path: str, worker_script: str, split_graph: bool, output_file: str, scale: float):
        self.worker_script = worker_script
        self.n_workers = n_workers
        self.output_file = output_file

        # Create queue
        self.server_queue = Queue()

        # Start server with queue
        self.server = Process(target=Server, args=(self.server_queue,))
        self.server.start()

        # Wait for server to send its hostname and port
        self.hostname, self.port = self.server_queue.get()

        # Split graph into sub graphs and send them to the workers
        self.workers = self.create_workers(graph_path, split_graph)

        # Can be used to handle incoming messages from the server
        self.message_handler_interface = {
            message.ALIVE: self.handle_alive,
            message.REGISTER: self.handle_register,
            message.DEBUG: self.handle_debug,
            message.PROGRESS: self.handle_progress,
            message.JOB_COMPLETE: self.handle_job_complete
        }

        self.register_workers()
        self.send_meta_data_to_workers()
        self.goal_size = self.get_goal_size(scale)

        # Run master until stopped
        try:
            self.run()
        except KeyboardInterrupt:
            self.terminate_workers()
            self.server.terminate()

    def get_goal_size(self, scale: float):
        return sum([get_number_of_lines(worker['sub-graph-path']) for worker in self.workers.values()]) * scale

    def process_graph(self, graph_path: str, split_graph: bool) -> [MetaData]:
        """
        Divides the graph into `number_of_workers` sub graphs and writes each chunk to a separate file

        :param graph_path: Path to the file containing the entire graph
        :param split_graph: Should the graph be split
        :return: List of paths to the created chunks
        """

        if split_graph:
            # Split graph into self.n_workers sub graphs
            workers = self.split_graph(graph_path)

            # Add reverse edges to sub graphs
            self.make_sub_graphs_bidirectional(graph_path, workers)

            # Sort sub graphs
            self.sort_sub_graphs(workers)

            # Update meta data
            self.update_meta_data(workers)

        else:
            # TODO do not duplicate data
            workers = {}
            for worker_id in range(self.n_workers):
                workers[worker_id] = {
                    'sub-graph-path': graph_path,
                    'meta-data': MetaData(
                        worker_id=worker_id,
                        number_of_edges=get_number_of_lines(graph_path),
                        min_vertex=get_start_vertex(get_first_line(graph_path)),
                        max_vertex=get_start_vertex(get_last_line(graph_path))
                    )
                }

        return workers

    def split_graph(self, graph_path) -> dict:
        workers = {}

        f = open(graph_path, "r")
        for worker_id, sub_graph in enumerate(read_in_chunks(f, self.n_workers)):
            sub_graph_path = write_chunk(worker_id, sub_graph)

            workers[worker_id] = {
                'sub-graph-path': sub_graph_path,
                'meta-data': MetaData(
                    worker_id=worker_id,
                    number_of_edges=get_number_of_lines(sub_graph_path),
                    min_vertex=get_start_vertex(get_first_line(sub_graph_path)),
                    max_vertex=get_start_vertex(get_last_line(sub_graph_path))
                )
            }
        f.close()

        return workers

    @staticmethod
    def make_sub_graphs_bidirectional(graph_path: str, workers: dict):
        combined_meta_data = CombinedMetaData([worker['meta-data'] for worker in workers.values()])

        f = open(graph_path, "r")
        for edge in read_as_reversed_edges(f):
            start_vertex = get_start_vertex(edge)

            if start_vertex < combined_meta_data.bottom_layer.min_vertex:
                worker_id = combined_meta_data.bottom_layer.worker_id
            elif start_vertex > combined_meta_data.top_layer.max_vertex:
                worker_id = combined_meta_data.top_layer.worker_id
            else:
                worker_id = combined_meta_data.get_worker_id_that_has_vertex(start_vertex)

            append_edge(
                path=workers[worker_id]['sub-graph-path'],
                edge=edge
            )
        f.close()

    @staticmethod
    def sort_sub_graphs(workers: dict):
        for worker in workers.values():
            sort_file(worker['sub-graph-path'])

    @staticmethod
    def update_meta_data(workers: dict):
        for worker_id in workers.keys():
            sub_graph_path = workers[worker_id]['sub-graph-path']

            workers[worker_id]['meta-data'] = MetaData(
                worker_id=worker_id,
                number_of_edges=get_number_of_lines(sub_graph_path),
                min_vertex=get_start_vertex(get_first_line(sub_graph_path)),
                max_vertex=get_start_vertex(get_last_line(sub_graph_path))
            )

    def create_workers(self, graph_path, split_graph) -> dict:
        """
        Creates `self.n_workers` workers
        :return: Dictionary containing info about each worker
        """
        workers = self.process_graph(graph_path, split_graph)

        for worker_id in workers.keys():
            workers[worker_id]['progress'] = 0
            workers[worker_id]['last-alive'] = None
            workers[worker_id]['job-complete'] = False
            workers[worker_id]['downscaled-sub-graph-path'] = None
            workers[worker_id]['process'] = command_line.setup_worker(
                self.worker_script,
                worker_id,
                self.hostname,
                self.port,
                workers[worker_id]['sub-graph-path']
            )

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
        # print(f"Worker {worker_id} is still alive")

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

    def handle_debug(self, worker_id, debug_message):
        print(f"Worker {worker_id}: {debug_message}")

    def handle_progress(self, worker_id, number_of_edges):
        self.workers[worker_id]['progress'] = number_of_edges

    def handle_job_complete(self, worker_id, output_path):
        self.workers[worker_id]['job-complete'] = True
        self.workers[worker_id]['downscaled-sub-graph-path'] = output_path

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

    def send_message_to_all_workers(self, message_to_send):
        for worker_id, worker in self.workers.items():
            sockets.send_message(*worker['meta-data'].get_connection_info(), message_to_send)

    def send_meta_data_to_workers(self):
        self.send_message_to_all_workers(
            message.write_meta_data([worker['meta-data'].to_dict() for worker in self.workers.values()])
        )

    def total_progress(self):
        return sum([worker['progress'] for worker in self.workers.values()])

    def print_progress(self):
        stdout.write('\r')
        stdout.write(f"{self.total_progress() / self.goal_size * 100}%")
        stdout.flush()

    def all_workers_done(self):
        for worker in self.workers.values():
            if not worker['job-complete']:
                return False

        return  True

    def wait_for_workers_to_complete(self):
        while not self.all_workers_done():
            self.handle_queue()

    def handle_queue(self):
        while self.message_in_queue():
            status, *args = self.get_message_from_queue()
            self.message_handler_interface[status](*args)

    def create_graph(self):
        graph = DistributedGraph(distributed=False)
        for worker in self.workers.values():
            graph.load_from_file(worker['downscaled-sub-graph-path'])

        return graph

    def run(self):
        """
        Runs the master
        """

        while self.total_progress() < self.goal_size:
            sleep(0.1)
            self.handle_queue()
            self.print_progress()
        print("\nJob complete")

        self.send_message_to_all_workers(message.write_job_complete())
        self.wait_for_workers_to_complete()
        self.terminate_workers()
        self.server.terminate()

        graph = self.create_graph()
        print(graph)
        graph.write_to_file(self.output_file)

