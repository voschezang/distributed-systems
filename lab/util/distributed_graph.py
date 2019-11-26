import pickle
from lab.util.graph import *


class GhostVertex():
    def __init__(self, label, combined_meta_data):
        self.label = label
        self.worker_id = combined_meta_data.get_worker_id_that_has_vertex(label)
        self.host = combined_meta_data.combined_meta_data[self.worker_id].host
        self.port = combined_meta_data.combined_meta_data[self.worker_id].port

    def __str__(self):
        return str(self.label)

    def __repr__(self):
        return "Vertex " + str(self)

    def __hash__(self):
        return hash(str(self))


class DistributedGraph(Graph):
    def __init__(self, min_vertex=0, max_vertex=float("inf"), combined_meta_data=None):
        super().__init__()
        self.min_vertex = min_vertex
        self.max_vertex = max_vertex
        self.combined_meta_data = combined_meta_data

    def addEdgeSet(self, edgeSet):
        number_of_edges = len(edgeSet)
        print("loading {} edges...".format(number_of_edges))
        for edge in edgeSet:
            v_1, v_2 = edge
            if not v_1 in self.vertices:
                vertex_1 = self.addVertex(v_1)
            if not v_2 in self.vertices:
                vertex_2 = self.addVertex(v_2)
            vertex_1 = self.vertices_dict[v_1]
            vertex_2 = self.vertices_dict[v_2]
            self.addEdge(vertex_1, vertex_2, False)

    def addVertex(self, label) -> Vertex:
        if self.min_vertex <= label <= self.max_vertex:
            v = Vertex(label)
        else:
            v = GhostVertex(label, self.combined_meta_data)
        self.vertices.append(v)
        self.vertices_dict[label] = v
        self.edges[v] = []
        return v


def distributed_graph_from_file(filename='graph.txt', min_vertex=0, max_vertex=float("inf"), combined_meta_data=None):
    edge_set = []
    with open(filename) as file:
        for line in file:
            v_1, v_2 = line[:-1].split(" ")
            edge_set.append([int(v_1), int(v_2)])
    graph = DistributedGraph(min_vertex, max_vertex, combined_meta_data)
    graph.addEdgeSet(edge_set)

    return graph
