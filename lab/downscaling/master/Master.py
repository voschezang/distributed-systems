from lab.downscaling.util import sockets, command_line
from lab.downscaling.util.file_io import read_in_chunks, write_chunk


class Master:
    def __init__(self, n_workers: int, graph_path: str, worker_script):
        self.workers = {}

        self.worker_script = worker_script
        self.n_workers = n_workers
        self.sub_graph_paths = self.divide_graph(graph_path)
        self.socket = sockets.bind("", 0)
        self.hostname = sockets.get_hostname()
        self.port = sockets.get_port(self.socket)
        self.create_workers()
        self.listen_for_messages()
        self.terminate_workers()

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

    def create_workers(self):
        for worker_id, sub_graph_path in enumerate(self.sub_graph_paths):
            self.workers[str(worker_id + 1)] = {
                'host': None,
                'port': None,
                'process': command_line.setup_worker(
                    self.worker_script,
                    worker_id + 1,
                    self.hostname,
                    self.port,
                    sub_graph_path
                )
            }

    def terminate_workers(self):
        print(self.workers)
        for worker_id, worker in self.workers.items():
            worker['process'].terminate()

    def listen_for_messages(self):
        for i in range(self.n_workers):
            worker_id, host, port = sockets.get_message(self.socket).split(', ')
            self.workers[worker_id]['host'] = host
            self.workers[worker_id]['port'] = port
