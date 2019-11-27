from lab.master.WorkerInterface import WorkerInterface
from lab.util.distributed_graph import DistributedGraph, ForeignVertex
from lab.downscaling.worker.RandomWalker import RandomWalker, ForeignVertexException
from numpy.random import randint
from lab.util import message, file_io


class Worker(WorkerInterface):
    def __init__(self, worker_id: int, master_host: str, master_port: int, graph_path: str):
        super().__init__(worker_id, master_host, master_port, graph_path)

        self.message_interface = {
            message.RANDOM_WALKER: self.handle_random_walker,
            message.JOB_COMPLETE: self.handle_job_complete
        }
        self.graph = DistributedGraph(worker_id, self.combined_meta_data, graph_path)
        self.random_walkers = [
            RandomWalker(self.get_random_vertex())
        ]
        self.collected_edges = {}
        self.terminate = False

        self.run()

    def get_random_vertex(self):
        return self.graph.vertices[
            randint(
                self.combined_meta_data[self.worker_id].min_vertex,
                self.combined_meta_data[self.worker_id].max_vertex
            )
        ]

    def handle_random_walker(self, vertex_label: int):
        self.random_walkers.append(RandomWalker(self.graph.vertices[vertex_label]))

    def handle_queue(self):
        while self.message_in_queue():
            status, *args = self.get_message_from_queue()
            self.message_interface[status](*args)

    def handle_job_complete(self, *args):
        self.terminate = True

    def send_random_walker_message(self, vertex: ForeignVertex):
        self.send_message_to_node(vertex.host, vertex.port, message.write_random_walker(vertex.label))

    def send_progress_message(self):
        self.send_message_to_master(message.write_progress(self.worker_id, len(self.collected_edges)))

    def send_job_complete(self, path):
        self.send_message_to_master(message.write_job_complete(self.worker_id, path))

    def run(self):
        """
        Runs the worker
        """

        step = 0
        while not self.terminate:
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

        output_file_path = f"/tmp/downscaled_sub_graph_{self.worker_id}.txt"
        file_io.write_to_file(
            path=output_file_path,
            data=[str(edge) + '\n' for edge in self.collected_edges.keys()]
        )
        file_io.sort_file(output_file_path)

        self.send_job_complete(output_file_path)
