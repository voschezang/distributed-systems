import numpy as np
import networkx as nx
from networkx.algorithms import approximation,  distance_measures, cluster, shortest_paths
from networkx.exception import NetworkXError

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


def average_clustering(graph: nx.Graph, approximate=True):
    # only for undirected graphs
    if approximate:
        return approximation.average_clustering(graph)

    return cluster.average_clustering(graph)


def diameter(graph: nx.Graph):
    """ Diameter or max. eccentricity of a graph
    Returns float or inf
    """
    try:
        # graph must be connected and undirected
        return distance_measures.diameter(graph)
    except NetworkXError:
        return np.inf


def aspl(graph: nx.Graph):
    """ Average Shortest Path Length
    Returns float or inf
    """
    try:
        # graph must be connected and undirected
        return shortest_paths.generic.average_shortest_path_length(graph)
    except NetworkXError:
        return np.inf


if __name__ == '__main__':
    filename = get_arg("--graph", assert_file,
                       default='data/facebook_head.txt')

    graph = Graph()
    graph.load_from_file(filename)
    result = degree_distribution(graph, 'outdegree')
    print(f'mean degree {result["mean"]}')

    graph.init_inverse_edges()
    result = degree_distribution(graph, 'indegree')
    print(f'mean degree {result["mean"]}')

    # only for undirected graphs
    graph = nx.readwrite.edgelist.read_edgelist(
        filename, nodetype=int, create_using=nx.Graph)

    k = average_clustering(graph)
    print(f'avg clustering coef: {k}')

    d = diameter(graph)
    print(f'diameter: {d}')

    s = aspl(graph)
    print(f'shortest path: {s}')
