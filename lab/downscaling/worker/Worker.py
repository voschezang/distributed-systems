from lab.master.WorkerInterface import WorkerInterface
from lab.util.distributed_graph import DistributedGraph, ForeignVertex, Vertex, Edge
from lab.downscaling.worker.RandomWalker import RandomWalker, ForeignVertexException
from numpy.random import randint, random
from numpy import array
from lab.util import message, file_io, validation


class Worker(WorkerInterface):
    def __init__(self, worker_id: int, master_host: str, master_port: int, graph_path: str, scale: float, method: str, output_file: str):
        super().__init__(worker_id, master_host, master_port, graph_path)

        self.message_interface = {
            message.RANDOM_WALKER: self.handle_random_walker,
            message.FINISH_JOB: self.handle_finish_job
        }

        self.output_file = output_file

        if method == "random_edge":
            self.scale = scale
            self.edges = []
            with open(graph_path) as file:
                for line in file:
                    vertex1_label, vertex2_label = file_io.parse_to_edge(line)
                    self.edges.append(Edge(Vertex(vertex1_label), Vertex(vertex2_label)))
            self.edges = array(self.edges)
            self.run_random_edge()
        elif method == "random_walk":
            self.graph = DistributedGraph(
                worker_id, self.combined_meta_data, graph_path)
            self.random_walkers = [
                RandomWalker(self.get_random_vertex())
            ]
            self.collected_edges = {}

            self.run_random_walk()

    def build_from_backup(self):
        try:
            validation.assert_file('', self.output_file)
        except AssertionError:
            return

        f = open(self.output_file)
        for line in f.readline():
            self.collected_edges[line] = True
        f.close()

    def get_random_vertex(self):
        return self.graph.vertices[
            randint(
                self.combined_meta_data[self.worker_id].min_vertex,
                self.combined_meta_data[self.worker_id].max_vertex
            )
        ]

    def handle_random_walker(self, vertex_label: int):
        self.random_walkers.append(RandomWalker(
            self.graph.vertices[vertex_label]))

    def send_random_walker_message(self, vertex: ForeignVertex):
        self.send_message_to_node(
            vertex.host, vertex.port, message.write_random_walker(vertex.label))

    def send_progress_message(self):
        self.send_message_to_master(message.write_progress(
            self.worker_id, len(self.collected_edges)))

    def run_random_edge(self):
        """
        Runs the worker
        """
        self.collected_edges = self.edges[random(len(self.edges)) < self.scale]
        file_io.write_to_file(
            path=self.output_file,
            data=[str(edge) + '\n' for edge in self.collected_edges]
        )
        file_io.sort_file(self.output_file)
        self.send_job_complete()

    def run_random_walk(self):
        """
        Runs the worker
        """

        step = 0
        while not self.cancel:
            for random_walker in self.random_walkers:
                try:
                    edge = random_walker.step()
                    self.collected_edges[edge] = True
                except ForeignVertexException:
                    self.send_random_walker_message(random_walker.vertex)
                    self.random_walkers.remove(random_walker)

            self.handle_queue()
            step += 1

            if step % 1000 == 0:
                self.send_progress_message()

        file_io.write_to_file(
            path=self.output_file,
            data=[str(edge) + '\n' for edge in self.collected_edges.keys()]
        )
        file_io.sort_file(self.output_file)

        self.send_job_complete()
