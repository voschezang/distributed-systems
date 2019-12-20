import networkx as nx
import networkx.readwrite.edgelist
import os


def generate(graph_type='', V=None, E=None, WS_probablity=0.1):
    """ Generate a graph
    Depending on the graph type, the number of vertices (V) or edges (E) can
    be specified

    :param graph_type: any of 'complete'
    """

    if graph_type == 'complete':
        return nx.complete_graph(V)
    elif graph_type == 'BA':
        assert E > V
        m = round(E / V)  # n edges per vertex
        return nx.barabasi_albert_graph(n=V, m=m)
    elif graph_type == 'ER':
        # E = p V (V - 1)/2
        p = 2 * E / (V * (V - 1))
        return nx.erdos_renyi_graph(n=V, p=p)
    elif graph_type == 'WS':
        # small world
        assert E > V
        m = round(E / V)  # n edges per vertex
        return nx.watts_strogatz_graph(n=V, k=m, p=WS_probablity)
    else:
        raise ValueError


if __name__ == '__main__':
    # G = generate('BA', 10, 20)
    # print(G.edges)
    # G = generate('ER', 10, 20)
    # print(G.edges)
    # G = generate('WS', 10, 20)
    # print(G.edges)
    for n in [100, 1000]:
        for p in [0.1, 0.4]:
            G = nx.watts_strogatz_graph(n=n, k=2, p=p)
            path = f'data/WS/WS_n_{n}_p_{p}.txt'
            nx.readwrite.edgelist.write_edgelist(G, path)

            # os.system(
            # 'python lab/master/__init__.py --graph data/WS/WS_n_100_p_0.1.txt --worker-script lab/upscaling/worker/__init__.py --scale 1.1')
