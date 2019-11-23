from lab.util.graph import *
import numpy as np
from lab.util import message
import time
from lab.master.WorkerInterface import WorkerInterface


class Worker(WorkerInterface):
    def run(self):
            """
            Runs the worker
            """
            # self.send_debug_message(self.graph)
            self.g = graph_from_file(self.graph)
            self.send_debug_message(str(self.g))

            self.min_vertex = self.meta_data_of_all_workers[self.worker_id].min_vertex
            self.max_vertex = self.meta_data_of_all_workers[self.worker_id].max_vertex

            # self.send_debug_message(str(self.meta_data_of_all_workers[self.worker_id].min_vertex))
            scale = 0.5 # TODO: niet hardcoded maar arg
            cur_vertex = self.g.vertices[np.random.choice(len(self.g.vertices))]
            vertices = {cur_vertex}
            edges = set()
            goal_size = int(scale * self.g.n_edges) # TODO: moet van totale aantal edges zijn, niet van zijn eigen.

            while len(edges) < 44000:
                new_vertex = self.g.edges[cur_vertex][np.random.choice(len(self.g.edges[cur_vertex]))]


            #while True:
            #    time.sleep(1)
            #    self.send_message_to_master(message.write_alive(self.worker_id))

    # def __init__(self, graph, worker_id: int, master_host: str, master_port: int, graph_path: str):
        # super().__init__(worker_id, master_host, master_port, graph_path)
        # self.g = graph_from_file(graph_path)
        # self.g.load_from_file(graph)

    def downscale(self, scale=0.5, method="rw"):
        if method == "rw":
            self.random_walk(scale)
        else:
            pass

    def random_walk(self, scale):
        cur_vertex = self.graph.vertices[np.random.choice(len(self.graph.vertices))]
        vertices = {cur_vertex}
        edges = set()
        goal_size = int(scale * len(self.graph.vertices))
        while len(vertices) < goal_size:
            new_vertex = self.graph.edges[cur_vertex][np.random.choice(len(self.graph.edges[cur_vertex]))]
            vertices.add(new_vertex)
            if (cur_vertex, new_vertex) not in edges and (new_vertex, cur_vertex) not in edges: # set filtert blijbkaar niet automatisch?
                edges.add((cur_vertex, new_vertex))
            cur_vertex = new_vertex

        new_graph = Graph()
        for v in vertices:
            new_graph.addVertex(v.label)
        for e in edges:
            new_graph.addEdge(e[0], e[1])
        print(g)
        print(new_graph)


#if __name__ == '__main__':
    #g = Graph()
    #g.load_from_file('facebook_combined.txt')
    #g.save_as_pickle()
    #g = g.load_pickle()
    #worker = Worker(g)
    #worker.downscale()
    #print(g)
