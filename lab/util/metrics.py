import numpy as np

from lab.util.graph import Graph


def degree_distribution(graph: Graph):
    degrees = [graph.degree(vertex) for vertex in graph.vertices]
    # TODO check if log-bins yield better "performance"
    # if so, determine whethere sampling within bins should be done with log scale as well (in Algorithm.DegreeDistrubution)
    hist, bins = np.histogram(degrees, bins='auto', density=False)
    # normalize manually because the bins are not uniform
    bin_pmf = hist / len(degrees)
    return bin_pmf, bins.round().astype(int)
