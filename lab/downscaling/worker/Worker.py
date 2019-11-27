from lab.master.WorkerInterface import WorkerInterface
from lab.util.distributed_graph_jens import DistributedGraph
from lab.downscaling.worker.RandomWalker import RandomWalker, ForeignVertexException
from numpy.random import randint


class Worker(WorkerInterface):
    def __init__(self, worker_id: int, master_host: str, master_port: int, graph_path: str):
        super().__init__(worker_id, master_host, master_port, graph_path)

        self.graph = DistributedGraph(worker_id, self.combined_meta_data, graph_path)
        self.random_walkers = [
            RandomWalker(self.get_random_vertex())
        ]
        self.collected_edges = {}

        self.run()

    def get_random_vertex(self):
        return self.graph.vertices[
            randint(
                self.combined_meta_data[self.worker_id].min_vertex,
                self.combined_meta_data[self.worker_id].max_vertex
            )
        ]

    def run(self):
        """
        Runs the worker
        """

        # TODO: While true until JOB_COMPLETE
        while True:
            for random_walker in self.random_walkers:
                try:
                    edge = random_walker.step()
                    self.collected_edges[edge] = True
                except ForeignVertexException:
                    self.send_debug_message("Foreign vertex reached")
                    self.random_walkers.remove(random_walker)


