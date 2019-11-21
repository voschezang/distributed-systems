import numpy as np

from lab.util.graph import Graph


def degree_distribution(graph: Graph):
    # TODO use log-spaced bins, to increase accuracy for power-law like networks
    hist, bins = np.histogram(graph.vertices, bins=100, density=True)
    max_degree = bins[-1] + 1
    return hist, bins, max_degree
