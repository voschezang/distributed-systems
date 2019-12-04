import numpy as np
import networkx as nx
from networkx.algorithms import approximation, centrality, distance_measures, cluster, shortest_paths
from networkx.classes import function

from lab.util.argument_parser import get_arg
from lab.util.validation import assert_file
from lab.util.graph import Graph


def degree_distribution(graph: Graph, degree_attr='degree', bins='auto'):
    """ Returns a dict with a statistical summary of the graph degree

    :param degree_attr: attr or func that returns the bi/in/out-degree of a
                        single vertex
                        e.g. indegree, outdegree, bidegree, rel_degree

    :param bins: int or str     arg used in np.histogram
    """
    degrees = np.array([getattr(graph, degree_attr)(vertex)
                        for vertex in graph.vertices])
    # TODO check if log-bins yield better "performance"
    # if so, determine whethere sampling within bins should be done with log scale as well (in Algorithm.DegreeDistrubution)
    hist, bins = np.histogram(degrees, bins=bins, density=False)
    # normalize manually because the bins are not uniform
    bin_pmf = hist / len(degrees)
    return {'pmf': bin_pmf,
            'bins': bins.round().astype(int),
            'mean': np.mean(degrees),
            'std': np.std(degrees)
            }


if __name__ == '__main__':
    filename = get_arg("--graph", assert_file,
                       default='data/facebook_head.txt')

    graph = Graph()
    graph.load_from_file(filename)
    bin_pmf, bins = degree_distribution(graph, 'outdegree')

    graph.init_inverse_edges()
    bin_pmf, bins = degree_distribution(graph, 'indegree')

    # graph = nx.readwrite.edgelist.read_edgelist(
    #     filename, nodetype=int, create_using=nx.DiGraph)
    #
    # node connectivity
    # strict lower bound on the actual number of node independent paths between two nodes
    # https://networkx.github.io/documentation/stable/reference/algorithms/generated/networkx.algorithms.approximation.connectivity.all_pairs_node_connectivity.html#id2
    # r = approximation.connectivity.all_pairs_node_connectivity(graph)
    # print(r)

    # hist = function.degree_histogram(graph)
    # print(type(hist))
    # print(graph)
    # clique = approximation.clique.max_clique(graph)

    # only for undirected graphs
    graph = nx.readwrite.edgelist.read_edgelist(
        filename, nodetype=int, create_using=nx.Graph)
    # size = approximation.clique.large_clique_size(graph)
    # print(size)

    # only for undirected graphs
    k = approximation.average_clustering(graph)
    print(f'avg clustering coef: {k}')
    k = cluster.average_clustering(graph)
    print(f'avg clustering coef: {k}')
    # k = cluster.generalized_degree(graph)

    # graph must be connected
    # d = distance_measures.diameter(graph)
    # print(f'diameter: {d}')

    # r = approximation.ramsey.ramsey_R2(graph)
    # print(r)

    # indegree = centrality.in_degree_centrality(graph)
    # print(indegree)

    # graph must be connected
    # s = shortest_paths.generic.average_shortest_path_length(graph)
    # print(f'shortest path: {s}')

    # print(graph.edges)
