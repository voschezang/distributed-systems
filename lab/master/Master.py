from lab.master.worker_info import WorkerInfoCollection, WorkerInfo
from lab.util import message, sockets
from lab.util.file_io import read_in_chunks, get_start_vertex, get_first_line, get_last_line, read_as_reversed_edges, \
    append_edge, get_number_of_lines, write_to_file, read_file
from lab.util.file_transfer import FileSender, UnexpectedChunkIndex, FileReceiver
from lab.util.server import Server
from time import time, sleep
from lab.util.meta_data import MetaData
from sys import stdout
from lab.util.distributed_graph import DistributedGraph
from uuid import uuid4
from math import ceil


class Master(Server):
    def __init__(self, n_workers: int, graph_path: str, worker_script: str, split_graph: bool, output_file: str,
                 scale: float, method: str):
        super().__init__()
        self.worker_script = worker_script
        self.n_workers = n_workers
        self.output_file = output_file
        self.method = method
        self.graph_path = graph_path
        self.scale = scale
        self.random_walker_counts_received = 0

        # Split graph into sub graphs and send them to the workers
        self.worker_info_collection = WorkerInfoCollection()
        self.create_workers(graph_path, split_graph)

        # Can be used to handle incoming messages from the server
        self.message_interface = {
            message.ALIVE: self.handle_alive,
            message.REGISTER: self.handle_register,
            message.DEBUG: self.handle_debug,
            message.PROGRESS: self.handle_progress,
            message.JOB_COMPLETE: self.handle_job_complete,
            message.RANDOM_WALKER_COUNT: self.handle_random_walker_count,
            message.MISSING_CHUNK: self.handle_missing_chunk,
            message.RECEIVED_FILE: self.handle_received_file,
            message.START_SEND_FILE: self.handle_start_send_file,
            message.END_SEND_FILE: self.handle_end_send_file,
            message.FILE_CHUNK: self.handle_file_chunk
        }

        self.register_workers()
        self.send_meta_data_to_workers()
        self.send_graphs_to_workers()

        self.goal_size = self.get_goal_size()

        # Run master until stopped
        try:
            self.run()
        except KeyboardInterrupt:
            self.terminate_workers()
            self.server.terminate()

    def get_goal_size(self):
        return self.worker_info_collection.get_total_number_of_edges() * self.scale

    def send_graphs_to_workers(self):
        for worker_id, worker_info in self.worker_info_collection.items():
            data = read_file(worker_info.input_sub_graph_path)
            self.send_data_to_worker(worker_id, data, message.GRAPH)

    def process_graph(self, graph_path: str, split_graph: bool) -> [MetaData]:
        """
        Divides the graph into `number_of_workers` sub graphs and writes each chunk to a separate file

        :param graph_path: Path to the file containing the entire graph
        :param split_graph: Should the graph be split
        :return: List of paths to the created chunks
        """

        if split_graph:
            # Split graph into self.n_workers sub graphs
            self.split_graph(graph_path)

            # Add reverse edges to sub graphs
            self.make_sub_graphs_bidirectional(graph_path)

            # Sort sub graphs
            self.worker_info_collection.sort_sub_graphs()

            # Update meta data
            self.worker_info_collection.update_meta_data()

        else:
            # TODO do not duplicate data
            for worker_id in range(self.n_workers):
                self.worker_info_collection[worker_id] = WorkerInfo(
                    worker_id=worker_id,
                    input_sub_graph_path=graph_path,
                    output_sub_graph_path=self.random_temp_file(f'output-worker-{worker_id}'),
                    meta_data=MetaData(
                        worker_id=worker_id,
                        number_of_edges=get_number_of_lines(graph_path),
                        min_vertex=get_start_vertex(get_first_line(graph_path)),
                        max_vertex=get_start_vertex(get_last_line(graph_path))
                    )
                )

    def split_graph(self, graph_path):
        f = open(graph_path, "r")
        for worker_id, sub_graph in enumerate(read_in_chunks(f, self.n_workers)):
            sub_graph_path = self.random_temp_file(f'input-worker-{worker_id}')
            write_to_file(sub_graph_path, sub_graph)

            self.worker_info_collection[worker_id] = WorkerInfo(
                worker_id=worker_id,
                input_sub_graph_path=sub_graph_path,
                output_sub_graph_path=self.random_temp_file(f'output-worker-{worker_id}'),
                meta_data=MetaData(
                    worker_id=worker_id,
                    number_of_edges=get_number_of_lines(sub_graph_path),
                    min_vertex=get_start_vertex(get_first_line(sub_graph_path)),
                    max_vertex=get_start_vertex(get_last_line(sub_graph_path))
                )
            )
        f.close()

    def make_sub_graphs_bidirectional(self, graph_path: str):
        combined_meta_data = self.worker_info_collection.get_combined_meta_data()

        f = open(graph_path, "r")
        for edge in read_as_reversed_edges(f):
            start_vertex = get_start_vertex(edge)

            if start_vertex < combined_meta_data.bottom_layer.min_vertex:
                worker_id = combined_meta_data.bottom_layer.worker_id
            elif start_vertex > combined_meta_data.top_layer.max_vertex:
                worker_id = combined_meta_data.top_layer.worker_id
            else:
                worker_id = combined_meta_data.get_worker_id_that_has_vertex(
                    start_vertex)

            append_edge(
                path=self.worker_info_collection[worker_id].input_sub_graph_path,
                edge=edge
            )
        f.close()

    def create_workers(self, graph_path, split_graph):
        """
        Creates `self.n_workers` workers
        :return: Dictionary containing info about each worker
        """
        self.process_graph(graph_path, split_graph)
        self.worker_info_collection.start_workers(
            self.worker_script,
            self.hostname,
            self.port,
            self.scale,
            self.method
        )

    @staticmethod
    def random_temp_file(prefix: str):
        return f'/tmp/{prefix}-{str(uuid4())}.txt'

    def terminate_workers(self):
        """
        Terminates the alive workers
        """

        self.broadcast(message.write(status=message.TERMINATE))
        self.handle_queue()
        # Wait for workers to shutdown their child-processes
        # TODO use confirmation msg
        sleep(0.5)
        self.handle_queue()
        self.worker_info_collection.terminate_workers()

    def handle_alive(self, worker_id):
        """
        Updates the last-alive value of the worker

        :param worker_id: Id of worker
        """

        self.worker_info_collection[worker_id].last_alive = time()

    def handle_register(self, worker_id, host, port):
        """
        Handles the registration of a worker

        :param worker_id: Id of worker
        :param host: Host of worker
        :param port: Port of worker
        """

        self.worker_info_collection[worker_id].meta_data.set_connection_info(host, port)
        self.handle_alive(worker_id)
        print(f"Registered worker {worker_id} on {host}:{port}")

    @staticmethod
    def handle_debug(worker_id, debug_message):
        print(f"Worker {worker_id}: {debug_message}")

    def handle_progress(self, worker_id, number_of_edges):
        self.worker_info_collection[worker_id].progress = number_of_edges

    def handle_job_complete(self, worker_id):
        self.worker_info_collection[worker_id].job_complete = True

    def handle_random_walker_count(self, worker_id, count):
        self.worker_info_collection[worker_id].random_walker_count = count
        self.random_walker_counts_received += 1

    def register_workers(self):
        for i in range(len(self.worker_info_collection)):
            status, *args = self.get_message_from_queue()
            self.handle_register(*args)

    def send_message_to_worker(self, worker_id, message: bytes):
        sockets.send_message(
            *self.worker_info_collection[worker_id].meta_data.get_connection_info(),
            message
        )

    def handle_missing_chunk(self, worker_id, file_type, index):
        self.worker_info_collection[worker_id].file_senders[file_type].index = index

    def handle_received_file(self, worker_id, file_type):
        self.worker_info_collection[worker_id].file_senders[file_type].target_received_file = True

    def send_data_to_worker(self, worker_id: int, data: list, file_type: int):
        self.worker_info_collection[worker_id].file_senders[file_type] = FileSender(worker_id, file_type, data)
        file_sender = self.worker_info_collection[worker_id].file_senders[file_type]

        self.send_message_to_worker(worker_id, message.write_start_send_file(worker_id, file_type, len(file_sender.messages)))
        while not file_sender.target_received_file:
            if file_sender.complete_file_send:
                self.send_message_to_worker(worker_id, message.write_end_send_file(worker_id, file_type))
                sleep(0.1)
            else:
                self.send_message_to_worker(worker_id, file_sender.get_next_message())

            self.handle_queue()

        self.worker_info_collection[worker_id].file_senders[file_type] = None

    def handle_start_send_file(self, worker_id, file_type, number_of_chunks):
        self.worker_info_collection[worker_id].file_receivers[file_type] = FileReceiver(number_of_chunks)

    def handle_file_chunk(self, worker_id, file_type, index, chunk):
        try:
            self.worker_info_collection[worker_id].file_receivers[file_type].receive_chunk(index, chunk)
        except UnexpectedChunkIndex as e:
            self.send_message_to_worker(worker_id, message.write_missing_chunk(worker_id, file_type, e.expected_index))
        except AttributeError:
            return

    def handle_end_send_file(self, worker_id, file_type):
        try:
            self.worker_info_collection[worker_id].file_receivers[file_type].handle_end_send_file()
            self.send_message_to_worker(worker_id, message.write_received_file(worker_id, file_type))

            if file_type == message.BACKUP:
                self.handle_backup(worker_id)

        except UnexpectedChunkIndex as e:
            self.send_message_to_worker(worker_id, message.write_missing_chunk(worker_id, file_type, e.expected_index))
        except AttributeError:
            return

    def handle_backup(self, worker_id):
        new_edges = self.worker_info_collection[worker_id].file_receivers[message.BACKUP].file

        for edge in new_edges:
            if edge == '':
                continue

            self.worker_info_collection[worker_id].backup.append(edge + '\n')

        self.worker_info_collection[worker_id].file_receivers[message.BACKUP] = None

    def broadcast(self, message, allow_connection_refused: bool = False):
        for worker_info in self.worker_info_collection.values():
            if worker_info.is_registered():
                if allow_connection_refused:
                    try:
                        sockets.send_message(*worker_info.meta_data.get_connection_info(), message)
                    except ConnectionRefusedError:
                        continue
                else:
                    sockets.send_message(*worker_info.meta_data.get_connection_info(), message)

    def send_meta_data_to_workers(self, allow_connection_refused: bool = False):
        self.broadcast(message.write_meta_data([
            worker_info.meta_data.to_dict() for worker_info in self.worker_info_collection.values()
        ]), allow_connection_refused)

    def total_progress(self):
        return self.worker_info_collection.get_progress()

    def print_progress(self):
        stdout.write('\r')
        stdout.write(f"{self.total_progress() / self.goal_size * 100:0.5f}% \t")
        stdout.flush()

    def wait_for_workers_to_complete(self):
        while not self.worker_info_collection.all_workers_done():
            sleep(0.01)
            self.handle_queue()

    def create_graph(self):
        graph = DistributedGraph(distributed=False)
        for worker_info in self.worker_info_collection.values():
            graph.load_from_list(worker_info.backup)

        return graph

    def wait_for_random_walker_counts(self, expected_number: int):
        while self.random_walker_counts_received < expected_number:
            self.handle_queue()
            sleep(0.01)
        self.random_walker_counts_received = 0

    def wait_for_worker_to_register(self, worker_id):
        while not self.worker_info_collection[worker_id].is_registered():
            self.handle_queue()
            sleep(0.01)

    def pause_workers(self):
        self.broadcast(message.write_worker_failed(), allow_connection_refused=True)

    def continue_workers(self):
        self.broadcast(message.write_continue(), allow_connection_refused=True)

    def get_failed_workers(self):
        if len([worker_id for worker_id in self.worker_info_collection.keys() if not self.worker_info_collection[worker_id].is_alive()]) == 0:
            return []

        return [worker_id for worker_id, worker_info in self.worker_info_collection.items() if not sockets.is_alive(*worker_info.meta_data.get_connection_info())]

    def control_workers(self):
        failed_workers = self.get_failed_workers()

        if len(failed_workers) == 0:
            return

        print("\n\n")

        # Update connection info
        for worker_id in failed_workers:
            print(f"Worker {worker_id} died")
            self.worker_info_collection[worker_id].meta_data.set_connection_info(None, None)

        print(f"Pausing workers")
        self.pause_workers()

        print("Waiting for random walker counts")
        self.wait_for_random_walker_counts(len(self.worker_info_collection) - len(failed_workers))

        random_walkers_to_restart = len(self.worker_info_collection) - self.worker_info_collection.random_walker_count()

        for worker_id in failed_workers:
            print(f"Restarting worker {worker_id}")

            number_of_random_walkers = ceil(random_walkers_to_restart / len(failed_workers))
            if number_of_random_walkers < 0:
                number_of_random_walkers = 0

            self.worker_info_collection[worker_id].start_worker(
                worker_script=self.worker_script,
                hostname=self.hostname,
                port=self.port,
                scale=self.scale,
                method=self.method,
                number_of_random_walkers=number_of_random_walkers,
                load_backup=1
            )

            random_walkers_to_restart -= number_of_random_walkers

        for worker_id in failed_workers:
            print(f"Waiting for worker {worker_id} to register")
            self.wait_for_worker_to_register(worker_id)

        print(f"Sending updated meta-data to workers")
        self.send_meta_data_to_workers(allow_connection_refused=True)

        for worker_id in failed_workers:
            self.send_data_to_worker(worker_id, self.worker_info_collection[worker_id].backup, message.BACKUP)

        self.continue_workers()
        print(f"Restart successful\n")

    def run(self):
        """
        Runs the master
        """
        self.broadcast(message.write_continue())

        if self.method == "random_walk":
            while self.total_progress() < self.goal_size:
                sleep(0.1)
                self.handle_queue()
                self.print_progress()
                self.control_workers()
            print("\nJob complete")
            self.broadcast(message.write_job(message.FINISH_JOB))

        self.wait_for_workers_to_complete()
        self.terminate_workers()
        self.server.terminate()

        graph = self.create_graph()
        graph.write_to_file(self.output_file)
