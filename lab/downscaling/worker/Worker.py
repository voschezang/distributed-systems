import numpy as np
from lab.master.WorkerInterface import WorkerInterface
from lab.util.distributed_graph_jens import DistributedGraph


class Worker(WorkerInterface):
    def __init__(self, worker_id: int, master_host: str, master_port: int, graph_path: str):
        super().__init__(worker_id, master_host, master_port, graph_path)
        self.graph = DistributedGraph(worker_id, self.meta_data_of_all_workers, graph_path)
        self.run()

    def run(self):
        """
        Runs the worker
        """
        self.send_debug_message(str(self.graph))
        # self.downscale()


        #while True:
        #    time.sleep(1)
        #    self.send_message_to_master(message.write_alive(self.worker_id))

    # def downscale(self, scale=0.5, method="rw"):
    #     if method == "rw":
    #         self.random_walk(scale)
    #     else:
    #         pass
    #
    # def random_walk(self, scale):
    #     vertices = [vertex for vertex in self.g.vertices if isinstance(vertex, Vertex)]  # filter GhostVertices
    #     cur_vertex = vertices[np.random.choice(len(vertices))]
    #     vertices = {cur_vertex}
    #     #edges = set()
    #     edges = {}
    #     goal_size = int(scale * self.meta_data_of_all_workers.combined_number_of_edges /
    #                     len(self.meta_data_of_all_workers.combined_meta_data))
    #
    #     while len(edges) < 100:  # int(44000/3):  # TODO: niet hardcoded
    #         #  print(self.g.edges)
    #         new_vertex = self.g.edges[cur_vertex][np.random.choice(len(self.g.edges[cur_vertex]))]
    #         if self.g.min_vertex <= new_vertex.label <= self.g.max_vertex:
    #             # print(new_vertex)
    #             vertices.add(new_vertex)
    #             cur_edge = Edge(cur_vertex, new_vertex)
    #             if cur_edge not in edges:
    #                 #edges.add((cur_vertex, new_vertex))
    #                 edges[cur_edge] = True
    #                 cur_vertex = new_vertex
    #
    #         else:
    #             pass
    #     new_graph = Graph()
    #     for v in vertices:
    #         new_graph.addVertex(v.label)
    #     for e in edges:
    #         new_graph.addEdge(e.vertex_1, e.vertex_2, False)
    #     print(self.g, new_graph)

    # def random_walk(self, scale):
    #     cur_vertex = self.graph.vertices[np.random.choice(len(self.graph.vertices))]
    #     vertices = {cur_vertex}
    #     edges = set()
    #     goal_size = int(scale * len(self.graph.vertices))
    #     while len(vertices) < goal_size:
    #         new_vertex = self.graph.edges[cur_vertex][np.random.choice(len(self.graph.edges[cur_vertex]))]
    #         vertices.add(new_vertex)
    #         if (cur_vertex, new_vertex) not in edges and (new_vertex, cur_vertex) not in edges: # set filtert blijbkaar niet automatisch?
    #             edges.add((cur_vertex, new_vertex))
    #         cur_vertex = new_vertex
    #
    #     new_graph = Graph()
    #     for v in vertices:
    #         new_graph.addVertex(v.label)
    #     for e in edges:
    #         new_graph.addEdge(e[0], e[1])
    #     print(g)
    #     print(new_graph)


#if __name__ == '__main__':
    #g = Graph()
    #g.load_from_file('facebook_combined.txt')
    #g.save_as_pickle()
    #g = g.load_pickle()
    #worker = Worker(g)
    #worker.downscale()
    #print(g)
