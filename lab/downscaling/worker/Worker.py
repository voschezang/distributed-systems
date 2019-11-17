from lab.proof_of_concept import *
import numpy as np

class Worker():
    def __init__(self, graph):
        self.graph = graph

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


if __name__ == '__main__':
    g = Graph()
    #g.load_from_file('facebook_combined.txt')
    #g.save_as_pickle()
    g = g.load_pickle()
    worker = Worker(g)
    worker.downscale()
    #print(g)
