import numpy as np

from lab.util import metrics
from lab.util.graph import Graph


class Algorithm:
    def __init__(self, v=0):
        self.v = v

    def run(self):
        raise NotImplementedError


class DegreeDistrubution(Algorithm):
    """ Scale up by first adding random nodes and then adding edges until the
    desired degree distribution has been reached.
    """

    def __init__(self, graph: Graph, v=0):
        super().__init__(v)
        self.graph = graph

        result = metrics.degree_distribution(graph)
        self.hist, self.bins = result['pmf'], result['bins']

        self.new_vertex_label = max(graph.vertices) + 1
        new_graph = graph.copy()
        self.new_graph = new_graph
        assert self.new_vertex_label not in new_graph.vertices
        if self.v:
            print(self.new_vertex_label,
                  self.new_vertex_label in new_graph.vertices)

    def run(self):
        """ Adds vertices
        Returns a new graph
        """
        for _ in self:
            pass
        return self.scaled_graph

    def __iter__(self):
        return self

    def __next__(self):
        # scale up
        # note that new vertices may connect to other new vertices
        self.add_vertex(self.hist, self.bins, self.new_graph)
        self.new_vertex_label += 1

    @property
    def scaled_graph(self):
        # filter old vertices
        for vertex in self.graph.vertices:
            self.new_graph.removeVertex(vertex)
        return self.new_graph

    def add_vertex(self, hist, bins, new_graph):
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
        new_vertex = new_graph.addVertex(self.new_vertex_label)
        for new_neighbour in new_neighbours:
            # TODO ignore duplicate edges and reflexive edges?
            new_graph.addEdge(new_vertex, new_neighbour)
            # adding bidirectional edges biasses the graph
            # counter this by removing a random edge
            collateral_vertex = np.random.choice(
                new_graph.edges[new_neighbour])
            new_graph.removeEdge(new_neighbour, collateral_vertex)
