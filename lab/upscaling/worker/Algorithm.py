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
        self.m_tilde = self.original_graph.n_edges * self.scale

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
        f_bi = [[f / (self.scale * self.original_graph.n_edges) for f in f_bi_row]
                for f_bi_row in f_bi_count]

        # Monster function N^4 -> [0, 1]. Sparse matrix, 
        # might be possible to drop the zero values
        f_corr_entries = 0
        for edge_1 in self.original_graph.raw_edges:
            vertex_1, vertex_2 = edge_1.vertex_1, edge_1.vertex_2
            vertex_1_degree = self.original_graph.degree(vertex_1)
            vertex_2_degree = self.original_graph.degree(vertex_2)
            for vertex_3 in self.original_graph.edges[vertex_1]:
                vertex_3_degree = self.original_graph.degree(vertex_3)
                for vertex_4 in self.original_graph.edges[vertex_2]:
                    vertex_4_degree = self.original_graph.degree(vertex_4)
                    f_corr_count[vertex_1_degree][vertex_2_degree][vertex_3_degree][vertex_4_degree] += 1
                    f_corr_entries += 1
        f_corr = [[[[f / f_corr_entries for f in f_1]
                    for f_1 in f_2]
                   for f_2 in f_3]
                  for f_3 in f_corr_count]
        return S_in, S_out, f_bi, f_corr

    def scaling(self, S):
        n_tilde, m_tilde = self.n_tilde, self.m_tilde
        S_tilde = []
        for s_1 in S:
            vertex_1, edge_1 = s_1
            edge_set = []
            for s_2 in S:
                vertex_2, edge_2 = s_2
                if vertex_1 == vertex_2:
                    vertex_3, vertex_4 = edge_2
                    if vertex_1 == vertex_3:
                        edge_set.append(vertex_4)
                    else:
                        edge_set.append(vertex_3)
            vertex_1 = copy.copy(vertex_1)
            S_tilde.append((vertex_1, edge_set))
        return S_tilde

    def node_synthesis(self, S_in_tilde, S_out_tilde, f_bi):
        f_bi_normalizing_factor = self.original_graph.n_edges * self.scale
        f_bi_normalizing_factor_inverse = 1.0 / f_bi_normalizing_factor
        S_bi_tilde = []
        while len(S_in_tilde)>0 and len(S_out_tilde)>0:
            incoming_vertex, incoming_edges = S_in_tilde.pop()
            incoming_degree = len(incoming_edges)
            _, outgoing_degree = max([(probability, degree) for degree, probability in enumerate(f_bi[incoming_degree]) if probability>0])
            for outgoing in S_out_tilde:
                outgoing_vertex, outgoing_edges = outgoing
                if len(outgoing_edges) == outgoing_degree:
                    S_out_tilde.remove(outgoing)
                    S_bi_tilde.append((incoming_edges, outgoing_edges))
                    
                    # update f_bi
                    f_bi[incoming_degree][outgoing_degree] -= f_bi_normalizing_factor_inverse
                    break
        return S_bi_tilde
    
    def manhattan(self, d1, d2):
        d11, d12 = d1
        d21, d22 = d2
        return abs(d11 - d21) + abs(d12 - d22)
    
    def correlation_function_scaling(self, f_corr, f_bi):
        f_corr_tilde = copy.deepcopy(f_corr)
        
        # if scale is integer, f_corr=f_corr_tilde
        if int(self.scale)==self.scale:
            return f_corr_tilde

        # print('f_corr', f_corr)
        # print('f_corr_tilde', f_corr_tilde)
        for vertex_1_degree_in, depth_1 in enumerate(f_corr_tilde):
            for vertex_1_degree_out, depth_2 in enumerate(depth_1):
                for vertex_2_degree_in, depth_3 in enumerate(depth_2):
                    for vertex_2_degree_out, probability in enumerate(depth_3):
                        # Three constaints, section 3.4.1
                        C1 = probability
                        C2_O = self.get_C2_O(f_corr, vertex_1_degree_in, vertex_1_degree_out)
                        C2_I = self.get_C2_I(f_corr, vertex_2_degree_in, vertex_2_degree_out)
                        C2 = min(C2_O, C2_I) / self.m_tilde
                        C3 = (self.original_graph.n_edges * self.original_graph.n_edges * f_bi[vertex_1_degree_in][vertex_1_degree_out] * f_bi[vertex_2_degree_in][vertex_2_degree_out]) / self.m_tilde - f_corr_tilde[vertex_1_degree_in][vertex_1_degree_out][vertex_2_degree_in][vertex_2_degree_out]
                        increment = min(C1, C2, C3)
                        f_corr_tilde[vertex_1_degree_in][vertex_1_degree_out][vertex_2_degree_in][vertex_2_degree_out] += increment
        # print('f_corr_tilde', f_corr_tilde)
        return f_corr_tilde
    
    def get_C2_O(self, f_corr, degree_in, degree_out):
        O =  0
        for degrees in f_corr[degree_in][degree_out]:
            for probability in degrees:
                O += probability

        return self.original_graph.n_edges * O
    
    def get_C2_I(self, f_corr, degree_in, degree_out):
        I =  0
        for degrees in f_corr:
            for probability in degrees:
                I += probability[degree_in][degree_out]

        return self.original_graph.n_edges * I
        

    def edge_synthesis(self, S_bi_tilde, f_corr, f_bi):
        f_corr_tilde = self.correlation_function_scaling(f_corr, f_bi)
        G_tilde = Graph()
        v = 0
        all_vertices = []
        for _ in S_bi_tilde:
            vertex = G_tilde.addVertex(v)
            all_vertices.append(vertex)
            v += 1

        v = 0
        while len(S_bi_tilde) > 0:
            print(len(S_bi_tilde))
            vertex_object = all_vertices.pop()
            incoming_nodes, outgoing_nodes = S_bi_tilde.pop()
            degree = len(set(incoming_nodes) | set(outgoing_nodes) - set([vertex_object]))
            to_be_linked = degree
            v_2 = 0
            for vertex_2, adjacent in zip(all_vertices, S_bi_tilde):
                if to_be_linked==0:
                    break
                incoming_nodes_2, outgoing_nodes_2 = adjacent
                if vertex != v_2:
                    G_tilde.addEdge(vertex_object, vertex_2)
                    to_be_linked -= 1
                    degree_2 = len(set(incoming_nodes_2) | set(outgoing_nodes_2) - set([v_2]))
                    if G_tilde.degree(vertex_2)==degree_2:
                        all_vertices.remove(vertex_2)
                        S_bi_tilde.remove(adjacent)
                        # print(vertex_2, 'is saturated')
                        break
                v_2 += 1
            print(G_tilde)
            v += 1
            
        return G_tilde

    def run(self):
        n_tilde, m_tilde = self.n_tilde, self.m_tilde
        S_in, S_out, f_bi, f_corr = self.decompose()
        S_in_tilde = self.scaling(S_in)
        S_out_tilde = self.scaling(S_out)
        S_bi_tilde = self.node_synthesis(S_in_tilde, S_out_tilde, f_bi)
        G_tilde = self.edge_synthesis(S_bi_tilde, f_corr, f_bi)
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
        S_in_test = [(vertex_1, g.edges[vertex_1])
                     for vertex_1 in g.vertices]
        S_out_test = [(vertex_1, g.edges[vertex_1])
                     for vertex_1 in g.vertices]
        
        # Probability function, should add up to 1.
        self.assertAlmostEqual(sum([x for y in f_bi for x in y]), 1 / scale)
        self.assertAlmostEqual(sum([x for v in f_corr for z in v for y in z for x in y]), 1)

        f_bi_test = [[0.0,0.0,0.0,0.0], [0.0, 0.0, 0.0, 0.0625], [0.0, 0.0, 0.125, 0.125], [0.0, 0.0625, 0.125,0.0]]
        f_corr_test = [[[[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]]], [[[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]]], [[[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.05263157894736842, 0.05263157894736842], [0.0, 0.0, 0.05263157894736842, 0.05263157894736842]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.10526315789473684, 0.21052631578947367, 0.0], [0.0, 0.10526315789473684, 0.21052631578947367, 0.0]]], [[[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.05263157894736842], [0.0, 0.0, 0.0, 0.10526315789473684], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]], [[0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0]]]]
        # self.assertEqual(S_in, S_in_test)
        # self.assertEqual(S_out, S_out_test)
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

        S_in_tilde_test = scale * [(vertex_1, [(vertex_1, vertex_2) for vertex_2 in g.edges[vertex_1]])
                     for vertex_1 in g.vertices if len(g.edges[vertex_1])>0]
        S_out_tilde_test = scale * [(vertex_1, [(vertex_1, vertex_2) for vertex_2 in g.edges[vertex_1]])
                     for vertex_1 in g.vertices if len(g.edges[vertex_1])>0]
        
        self.assertEqual(len(S_in_tilde), len(S_in_tilde_test))
        self.assertEqual(len(S_out_tilde), len(S_out_tilde_test))

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

        for element in S_bi_tilde:
            self.assertEqual(len(element), 2)
        self.assertEqual(len(S_bi_tilde), 8)

    def test_edge_synthesis(self):
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
        S_in_tilde = gscaler.scaling(S_in)
        S_out_tilde = gscaler.scaling(S_out)
        S_bi_tilde = gscaler.node_synthesis(S_in_tilde, S_out_tilde, f_bi)
        f_corr_tilde = gscaler.correlation_function_scaling(f_corr, f_bi)
        # todo add test f_corr_tilde

        G_tilde = gscaler.edge_synthesis(S_bi_tilde, f_corr, f_bi)
        print(G_tilde)
        print(G_tilde.edges)
        self.assertEqual(G_tilde.n_vertices, 8)
        self.assertEqual(G_tilde.n_edges, 16)