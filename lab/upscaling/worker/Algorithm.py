import numpy as np

from lab.util import metrics
from lab.util.graph import Graph


class Algorithm:
    def __init__(self):
        print('Algorithm init')

    def run(self):
        raise NotImplementedError


class DegreeDistrubution(Algorithm):
    """ Scale up by first adding random nodes and then adding edges until the
    desired degree distribution has been reached.
    """

    def run(self, graph: Graph, scale=2):
        """ Adds vertices
        Returns a new graph
        """
        assert scale > 1
        old_size = graph.n_vertices
        new_size = round(old_size * scale)  # worker.arguments['scale']
        # TODO return new vertices
        print('starting algorithm. Old size: {} New size: {}'.format(
            old_size, new_size))
        hist, bins, max_degree = metrics.degree_distribution(graph)
        new_vertex_label = old_size
        new_graph = graph.copy()

        # scale up
        for _ in range(new_size - old_size):
            # note that new vertices may connect to other new vertices
            self.add_vertex(hist, bins, new_vertex_label, new_graph)
            new_vertex_label += 1

        # filter old vertices
        for vertex in graph.vertices:
            new_graph.removeVertex(vertex)
        return new_graph

    def add_vertex(hist, bins, new_vertex_label, new_graph):
        """ Adds a single vertex
        """
        bin = np.random.choice(
            range(bins.size), p=hist)
        new_vertex_degree = np.random.randint(
            low=bins[bin], high=bins[bin + 1])
        print('adding node with degree {}'.format(new_vertex_degree))

        neighbours = np.random.choice(
            new_graph.vertices, size=new_vertex_degree, replace=False)
        new_vertex = new_graph.addVertex(new_vertex_label)
        for neighbour in neighbours:
            new_graph.addEdge(new_vertex, neighbour)
            # adding bidirectional edges biasses the graph
            # counter this by removing a random edge
            collateral_vertex = np.random.choice(
                new_graph.edges[neighbour])
            new_graph.removeEdge(collateral_vertex)
