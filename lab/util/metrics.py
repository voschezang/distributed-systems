import numpy as np

from lab.util.graph import Graph


def degree_distribution(graph: Graph):
    # TODO use log-spaced bins, to increase accuracy for power-law like networks
    hist, bins = np.histogram(graph.vertices, n_bins=100, density=True)
    max_degree = bins[-2]
    print('bins', np.histogram(np.arange(99), n_bins=3))
    print(bins.size, 3)
    # max_degree = 0
    # for vertex in graph.vertices:
    #     max_degree = max(max_degree, graph.degree(vertex))

    # degree_count = []
    # for vertex in graph.vertices:
    #     degree = graph.degree(vertex)
    #     while degree >= len(degree_count):
    #         degree_count.append(0)
    #     degree_count[degree] += 1

    # max_degree = len(degree_count)
    # all_degrees = list(range(max_degree))
    # number_of_degrees = sum(degree_count)  # n vertices
    # new_vertex_label = old_size
    # degree_distribution = [d / number_of_degrees for d in degree_count]
    return hist, bins, max_degree,
