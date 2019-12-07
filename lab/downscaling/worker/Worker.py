from lab.master.WorkerInterface import WorkerInterface
from lab.util.distributed_graph import DistributedGraph, ForeignVertex, Vertex, Edge
from lab.downscaling.worker.RandomWalker import RandomWalker, ForeignVertexException
from numpy.random import randint, random
from numpy import array
from lab.util import message, file_io, validation
from lab.util.meta_data import CombinedMetaData, MetaData
from time import sleep


class Worker(WorkerInterface):
    def __init__(self, worker_id: int, master_host: str, master_port: int, graph_path: str, scale: float, method: str, output_file: str, number_of_random_walkers: int):
        super().__init__(worker_id, master_host, master_port, graph_path)

        self.message_interface = {
            message.META_DATA: self.handle_meta_data,
            message.RANDOM_WALKER: self.handle_random_walker,
            message.FINISH_JOB: self.handle_finish_job,
            message.WORKER_FAILED: self.handle_worker_failed,
            message.CONTINUE: self.handle_continue
        }

        self.output_file = output_file
        self.running = True

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
                RandomWalker(self.get_random_vertex()) for _ in range(number_of_random_walkers)
            ]
            self.collected_edges = self.build_from_backup()

            self.run_random_walk()

    def handle_meta_data(self, all_meta_data):
        self.combined_meta_data = CombinedMetaData([
            MetaData(
                worker_id=meta_data['worker_id'],
                number_of_edges=meta_data['number_of_edges'],
                min_vertex=meta_data['min_vertex'],
                max_vertex=meta_data['max_vertex'],
                host=meta_data['host'],
                port=meta_data['port']
            )
            for meta_data in all_meta_data
        ])

    def build_from_backup(self):
        try:
            validation.assert_file('', self.output_file)
        except AssertionError:
            return {}

        collected_edges = {}

        f = open(self.output_file, 'r')
        for line in f:
            collected_edges[line.strip()] = True
        f.close()

        return collected_edges

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

    def handle_continue(self):
        self.running = True

    def handle_worker_failed(self):
        self.running = False
        self.send_message_to_master(message.write_random_walker_count(self.worker_id, len(self.random_walkers)))

        while self.running:
            self.handle_queue()
            sleep(0.01)

    def send_random_walker_message(self, vertex: ForeignVertex):
        self.send_message_to_node(
            *self.combined_meta_data.get_connection_that_has_vertex(vertex.label),
            message.write_random_walker(vertex.label)
        )

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

        new_edges = []

        die_after_n_steps = randint(10000, 300000)

        step = 0
        while not self.cancel:
            self.handle_queue()

            for random_walker in self.random_walkers:
                try:
                    edge = random_walker.step()

                    if str(edge) not in self.collected_edges:
                        self.collected_edges[str(edge)] = True
                        new_edges.append(str(edge) + "\n")
                except ForeignVertexException:
                    try:
                        self.send_random_walker_message(random_walker.vertex)
                        self.random_walkers.remove(random_walker)
                    except ConnectionRefusedError:
                        continue

            step += 1

            if step % 1000 == 0:
                self.send_progress_message()
                file_io.append_to_file(
                    path=self.output_file,
                    data=new_edges
                )
                new_edges = []

            if step > die_after_n_steps:
                self.handle_terminate()
                return

        file_io.sort_file(self.output_file)

        self.send_job_complete()
