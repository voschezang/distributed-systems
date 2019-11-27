from typing import List, Dict

from lab.util import file_io
from lab.util.meta_data import CombinedMetaData


class ForeignVertex:
    def __init__(self, label: int, host: str, port: int):
        self.label = label
        self.host = host
        self.port = port

    def __str__(self):
        return str(self.label)

    def __hash__(self):
        return hash(str(self))

    def __repr__(self):
        return str(self)


class Vertex:
    def __init__(self, label: int):
        self.label = label
        self.edges: List[Edge] = []

    @property
    def degree(self):
        return len(self.edges)

    def add_edge(self, edge):
        assert isinstance(edge, Edge)
        self.edges.append(edge)

    def __str__(self):
        return str(self.label)

    def __hash__(self):
        return hash(str(self))

    def __repr__(self):
        return str(self)


class Edge:
    def __init__(self, from_vertex: Vertex, to_vertex: Vertex or ForeignVertex):
        self.from_vertex = from_vertex
        self.to_vertex = to_vertex

    def __str__(self):
        return f"{self.from_vertex} {self.to_vertex}"

    def __repr__(self):
        return str(self)


class DistributedGraph:
    def __init__(self, worker_id: int, combined_meta_data: CombinedMetaData, graph_path: str):
        self.vertices: Dict[int, Vertex] = {}
        self.foreign_vertices: Dict[int, ForeignVertex] = {}
        self.worker_id = worker_id
        self.combined_meta_data = combined_meta_data
        self.load_from_file(graph_path)

    @property
    def number_of_vertices(self):
        return len(self.vertices.keys())

    @property
    def number_of_edges(self):
        return sum([vertex.degree for vertex in self.vertices.values()])

    def has_vertex(self, vertex_label):
        return self.combined_meta_data[self.worker_id].has_vertex(vertex_label)

    def get_connection_that_has_vertex(self, vertex_label):
        return self.combined_meta_data.get_connection_that_has_vertex(vertex_label)

    def add_vertex(self, vertex_label) -> Vertex:
        vertex = Vertex(vertex_label)
        self.vertices[vertex_label] = vertex

        return vertex

    def add_foreign_vertex(self, vertex_label) -> ForeignVertex:
        vertex = ForeignVertex(vertex_label, *self.get_connection_that_has_vertex(vertex_label))
        self.foreign_vertices[vertex_label] = vertex

        return vertex

    def get_vertex(self, vertex_label: int):
        if self.has_vertex(vertex_label):
            try:
                return self.vertices[vertex_label]
            except KeyError:
                return self.add_vertex(vertex_label)
        else:
            try:
                return self.foreign_vertices[vertex_label]
            except KeyError:
                return self.add_foreign_vertex(vertex_label)

    def load_from_file(self, filename='graph.txt'):
        with open(filename) as file:
            for line in file:
                vertex1_label, vertex2_label = file_io.parse_to_edge(line)

                vertex1 = self.get_vertex(vertex1_label)
                vertex2 = self.get_vertex(vertex2_label)

                vertex1.add_edge(Edge(vertex1, vertex2))
        file.close()

    def save_to_file(self, path):
        edges = []
        for vertex in self.vertices.values():
            for edge in vertex.edges:
                edges.append(str(edge) + "\n")

        file_io.write_to_file(path, edges)

    def __str__(self):
        return f"Graph |V|={len(self.vertices.keys())}, |E|={self.number_of_edges}"

    def __repr__(self):
        return str(self)
