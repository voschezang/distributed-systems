import copy, math, unittest
import numpy as np
from lab.util.graph import Graph

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
            new_graph.addEdge(new_vertex, new_neighbour)
            # adding bidirectional edges biasses the graph
            # counter this by removing a random edge
            collateral_vertex = np.random.choice(
                new_graph.edges[new_neighbour])
            new_graph.removeEdge(new_neighbour, collateral_vertex)


class GScalerAlgorithm(Algorithm):
    """
        As described in GSCALER: synthetically scaling a graph.
        Currently only works for undirected graphs.
    """

    def __init__(self, original_graph, scale: int):
        self.original_graph = original_graph
        self.scale = scale
        self.n_tilde = len(self.original_graph.vertices) * self.scale
        self.m_tilde = self.original_graph.amountOfEdges * self.scale

    def decompose(self):
        S_in = []
        S_out = []
        f_bi_count = []
        f_corr_count = []

        # Generate placeholders
        graph_max_degree = self.original_graph.max_degree()
        for dim_1 in range(graph_max_degree + 1):
            f_bi_row = []
            f_corr_row_1 = []
            for dim_2 in range(graph_max_degree + 1):
                f_bi_row.append(0)
                f_corr_row_2 = []
                for dim_3 in range(graph_max_degree + 1):
                    f_corr_row_3 = []
                    for dim_4 in range(graph_max_degree + 1):
                        f_corr_row_3.append(0)
                    f_corr_row_2.append(f_corr_row_3)
                f_corr_row_1.append(f_corr_row_2)
            f_bi_count.append(f_bi_row)
            f_corr_count.append(f_corr_row_1)

        for vertex_1 in self.original_graph.vertices:
            for vertex_2 in self.original_graph.edges[vertex_1]:
                S_in.append((vertex_1, (vertex_1, vertex_2)))
                S_out.append((vertex_2, (vertex_2, vertex_1)))
                vertex_1_degree = self.original_graph.degree(vertex_1)
                vertex_2_degree = self.original_graph.degree(vertex_2)
                f_bi_count[vertex_1_degree][vertex_2_degree] += 1
        f_bi = [[f / (2 * self.original_graph.amountOfEdges) for f in f_bi_row]
                for f_bi_row in f_bi_count]

        # Monster function N^4 -> [0, 1]. Sparse matrix, 
        # might be possible to drop the zero values
        f_corr_entries = 0
        for edge_1 in self.original_graph.rawEdges:
            vertex_1, vertex_2 = edge_1.vertex_1, edge_1.vertex_2
            vertex_1_degree = self.original_graph.degree(vertex_1)
            vertex_2_degree = self.original_graph.degree(vertex_2)
            for vertex_3 in self.original_graph.edges[vertex_1]:
                vertex_3_degree = self.original_graph.degree(vertex_3)
                for vertex_4 in self.original_graph.edges[vertex_2]:
                    vertex_4_degree = self.original_graph.degree(vertex_4)
                    f_corr_count[vertex_1_degree][vertex_2_degree][vertex_3_degree][vertex_4_degree] += 1
                    f_corr_entries += 1
        f_corr = [[[[f / (f_corr_entries) for f in f_1]
                    for f_1 in f_2]
                   for f_2 in f_3]
                  for f_3 in f_corr_count]
        return S_in, S_out, f_bi, f_corr

    def scaling(self, S):
        n_tilde, m_tilde = self.n_tilde, self.m_tilde
        S_tilde = []
        for s in S:
            vertex_1, edge = s
            _, vertex_2 = edge
            for _ in range(self.scale):
                vertex_1 = copy.copy(vertex_1)
                vertex_2 = copy.copy(vertex_2)
                S_tilde.append((vertex_1, (vertex_1, vertex_2)))
        return S_tilde

    def node_synthesis(self, S_in_tilde, S_out_tilde, f_bi):
        S_bi_tilde = []
        while len(S_in_tilde)>0 and len(S_out_tilde)>0:
            for d1, f_bi_row in enumerate(f_bi):
                for d2, f_bi_d1_d2 in enumerate(f_bi_row):
                    print(d1, d2, self.manhattan(d1, d2))
                    while math.floor(f_bi_d1_d2 * self.m_tilde) > 0:
                        print('test')
        return S_bi_tilde
    
    def manhattan(self, d1, d2):
        d11, d12 = d1
        d21, d22 = d2
        return abs(d11 - d21) + abs(d12 - d22)

    def edge_synthesis(self, S_bi_tilde, f_corr):
        G_tilde = Graph()

        return G_tilde

    def run(self):
        n_tilde, m_tilde = self.n_tilde, self.m_tilde
        S_in, S_out, f_bi, f_corr = self.decompose()
        S_in_tilde = self.scaling(S_in)
        S_out_tilde = self.scaling(S_out)
        S_bi_tilde = self.node_synthesis(S_in_tilde, S_out_tilde, f_bi)
        G_tilde = self.edge_synthesis(S_bi_tilde, f_corr)
        return G_tilde


class GScalerAlgorithmTest(unittest.TestCase):
    def test_decompose(self):
        g = Graph()
        for i in range(5):
            g.addVertex(i)
        edges = [[0, 1], [1, 2], [2, 3], [0, 2]]
        g.addEdgeSet(edges)

        scale = 2
        # n_tilde = 8
        # m_tilde = 8
        gscaler = GScalerAlgorithm(g, scale)
        S_in, S_out, f_bi, f_corr = gscaler.decompose()
        S_in_test = [(vertex_1, (vertex_1, vertex_2))
                     for vertex_1 in g.vertices for vertex_2 in g.edges[vertex_1]]
        S_out_test = [(vertex_2, (vertex_2, vertex_1))
                      for vertex_1 in g.vertices for vertex_2 in g.edges[vertex_1]]
        f_bi_test = [[0.0,0.0,0.0,0.0], [0.0, 0.0, 0.0, 0.125], [0.0, 0.0, 0.25, 0.25], [0.0, 0.125, 0.25,0.0]]
        f_corr_test = [[[[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]]], [[[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]]], [[[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.05263157894736842, 0.05263157894736842], [0.0, 0.0, 0.05263157894736842, 0.05263157894736842]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.10526315789473684, 0.21052631578947367, 0.0], [0.0, 0.10526315789473684, 0.21052631578947367, 0.0]]], [[[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.05263157894736842], [0.0, 0.0, 0.0, 0.10526315789473684], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]]]]
        self.assertEqual(S_in, S_in_test)
        self.assertEqual(S_out, S_out_test)
        self.assertEqual(f_bi, f_bi_test)
        self.assertEqual(f_corr, f_corr_test)

    def test_scaling(self):
        g = Graph()
        for i in range(5):
            g.addVertex(i)
        edges = [[0, 1], [1, 2], [2, 3], [0, 2]]
        g.addEdgeSet(edges)

        scale = 2
        # n_tilde = 8
        # m_tilde = 8
        gscaler = GScalerAlgorithm(g, scale)
        S_in, S_out, _, _ = gscaler.decompose()
        S_in_tilde = gscaler.scaling(S_in)
        S_out_tilde = gscaler.scaling(S_out)

        S_in_tilde_test = 2 * [(vertex_1, (vertex_1, vertex_2))
                     for vertex_1 in g.vertices for vertex_2 in g.edges[vertex_1]]
        S_out_tilde_test = 2 * [(vertex_2, (vertex_2, vertex_1))
                      for vertex_1 in g.vertices for vertex_2 in g.edges[vertex_1]]

        self.assertEqual(set(S_in_tilde), set(S_in_tilde_test))
        self.assertEqual(set(S_out_tilde), set(S_out_tilde_test))

    def test_node_synthesis(self):
        g = Graph()
        for i in range(5):
            g.addVertex(i)
        edges = [[0, 1], [1, 2], [2, 3], [0, 2]]
        g.addEdgeSet(edges)

        scale = 2
        # n_tilde = 8
        # m_tilde = 8
        gscaler = GScalerAlgorithm(g, scale)
        S_in, S_out, f_bi, _ = gscaler.decompose()
        S_in_tilde = gscaler.scaling(S_in)
        S_out_tilde = gscaler.scaling(S_out)
        S_bi_tilde = gscaler.node_synthesis(S_in_tilde, S_out_tilde, f_bi)

    def test_edge_synthesis(self):
        pass
