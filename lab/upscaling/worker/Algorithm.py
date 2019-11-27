import numpy as np

from lab.util import metrics
from lab.util.graph import Graph


class Algorithm:
    def __init__(self, v=0):
        self.v = v

    def run(self, graph: Graph, scale=1):
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
        if self.v:
            print('starting algorithm. Old size: {} New size: {}'.format(
                old_size, new_size))
        hist, bins = metrics.degree_distribution(graph)
        new_vertex_label = max(graph.vertices) + 1
        new_graph = graph.copy()
        assert new_vertex_label not in new_graph.vertices
        if self.v:
            print(new_vertex_label, new_vertex_label in new_graph.vertices)

        # scale up
        for _ in range(new_size - old_size):
            # note that new vertices may connect to other new vertices
            self.add_vertex(hist, bins, new_vertex_label, new_graph)
            new_vertex_label += 1

        # filter old vertices
        for vertex in graph.vertices:
            new_graph.removeVertex(vertex)
        return new_graph

    def add_vertex(self, hist, bins, new_vertex_label, new_graph):
        """ Adds a single vertex
        """
        bin = np.random.choice(
            range(bins.size - 1), p=hist)
        if self.v:
            print(f'bins {bins[bin]} {bins[bin + 1]}')
        if bins[bin] != bins[bin + 1]:
            new_vertex_degree = np.random.randint(
                low=bins[bin], high=bins[bin + 1])
        else:
            new_vertex_degree = bins[bin]

        if self.v:
            print('adding node with degree {}'.format(new_vertex_degree))

        new_neighbours = np.random.choice(
            new_graph.vertices, size=new_vertex_degree, replace=False)
        new_vertex = new_graph.addVertex(new_vertex_label)
        for new_neighbour in new_neighbours:
            # TODO ignore duplicate edges and reflexive edges?
            new_graph.add_edge(new_vertex, new_neighbour)
            # adding bidirectional edges biasses the graph
            # counter this by removing a random edge
            collateral_vertex = np.random.choice(
                new_graph.edges[new_neighbour])
            new_graph.removeEdge(new_neighbour, collateral_vertex)
