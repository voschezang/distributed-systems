from lab.util.distributed_graph_jens import Vertex, ForeignVertex, Edge
from numpy.random import randint


class ForeignVertexException(Exception):
    def __init__(self, message, errors):

        # Call the base class constructor with the parameters it needs
        super(ForeignVertexException, self).__init__(message)

        # Now for your custom code...
        self.errors = errors


class RandomWalker:
    def __init__(self, vertex: Vertex):
        self.vertex = vertex

    def get_random_edge(self):
        return self.vertex.edges[randint(0, self.vertex.degree)]

    def step(self):
        if isinstance(self.vertex, ForeignVertex):
            raise ForeignVertexException(f"Foreign vertex reached: {str(self.vertex)}", None)
        else:
            edge = self.get_random_edge()
            self.vertex = edge.to_vertex
            return edge
