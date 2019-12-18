import pickle


class Vertex(int):
    def __new__(cls, label):
        # allow direct comparison, e.g. max(list(Vertex))
        return super(cls, cls).__new__(cls, label)

    def __str__(self):
        return str(self.label)

    def __repr__(self):
        return "Vertex " + str(self)

    def __hash__(self):
        return hash(str(self))

    @property
    def label(self):
        return int(self)


class Edge:
    def __init__(self, vertex_1, vertex_2):
        self.vertex_1 = vertex_1
        self.vertex_2 = vertex_2

    def __str__(self):
        return str(self.vertex_1) + ' - ' + str(self.vertex_2)

    def __repr__(self):
        return str(self)


class Graph:
    def __init__(self):
        self.vertices_dict = {}  # {vertex.label: vertex}
        self.vertices = []  # list(Vertex)
        self.edges = {}  # {Vertex: list(Vertex)}
        self.id = 0

    @property
    def raw_edges(self):
        for vertex_1 in self.edges:
            for vertex_2 in self.edges[vertex_1]:
                if vertex_2 > vertex_1:
                    yield Edge(vertex_1, vertex_2)

    @property
    def n_vertices(self):
        return len(self.vertices)

    @property
    def n_edges(self):
        return sum(map(len, self.edges.values()))

    @property
    def inverse_edges(self):
        try:
            return self._inverse_edges
        except AttributeError as e:
            print('inverse_edges must be called once with arg `recompute=True`', e)
            self.init_inverse_edges()
            return self._inverse_edges

    def init_inverse_edges(self):
        self._inverse_edges = {}
        for source, vertices in self.edges.items():
            for target in vertices:
                if target not in self._inverse_edges:
                    self._inverse_edges[target] = []
                self._inverse_edges[target].append(source)

    def addEdgeSet(self, edgeSet, bidirectional=True, ignore_duplicates=True):
        number_of_edges = len(edgeSet)
        print("loading {} edges...".format(number_of_edges))
        for edge in edgeSet:
            v_1, v_2 = edge
            if not v_1 in self.vertices:
                vertex_1 = self.addVertex(v_1)
            if not v_2 in self.vertices:
                vertex_2 = self.addVertex(v_2)
            vertex_1 = self.vertices_dict[v_1]
            vertex_2 = self.vertices_dict[v_2]
            if ignore_duplicates:
                try:
                    self.addEdge(vertex_1, vertex_2, bidirectional)
                except ValueError:
                    return

            else:
                self.addEdge(vertex_1, vertex_2,
                             bidirectional, ignore_duplicates)

    def addEdge(self, vertex_1: Vertex, vertex_2: Vertex, bidirectional=True,
                ignore_duplicates=False):
        if not vertex_1 in self.vertices:
            raise ValueError("Vertex %s not known in this graph" % vertex_1)
        if not vertex_2 in self.vertices:
            raise ValueError("Vertex %s not known in this graph" % vertex_2)
        if vertex_2 in self.edges[vertex_1]:
            if not ignore_duplicates:
                raise ValueError(
                    "Edge (%s, %s) already exists in this graph" % (vertex_1, vertex_2))
        for vertex in [vertex_1, vertex_2]:
            if vertex not in self.edges.keys():
                self.edges[vertex] = []

        e = Edge(vertex_1, vertex_2)
        self.edges[vertex_1].append(vertex_2)
        if bidirectional and vertex_1 != vertex_2:
            self.edges[vertex_2].append(vertex_1)
        return e

    def addVertex(self, label) -> Vertex:
        v = Vertex(label)
        self.vertices.append(v)
        self.vertices_dict[label] = v
        self.edges[v] = []
        return v

    def removeVertex(self, v: Vertex):
        # Remove vertex and outgoing edges
        self.vertices.remove(v)
        try:
            del self.edges[v]
            del self.vertices_dict[v.label]
        except KeyError:
            pass

    def removeEdge(self, a: Vertex, b: Vertex):
        # remove unidirectional edge but do not remove any
        # (possibly unconnectec) vertices
        self.edges[a].remove(b)

    def copy(self):
        H = Graph()
        H.vertices = list(self.vertices)
        H.edges = dict((v, list(self.edges[v])) for v in self.edges)
        H.id = int(self.id) + 1
        return H

    def degree(self, vertex):
        return len(self.edges[vertex])

    def indegree(self, vertex):
        return len(self.inverse_edges[vertex])

    def outdegree(self, vertex):
        return len(self.edges[vertex])

    def bidegree(self, vertex):
        return self.indegree(vertex) + self.outdegree(vertex)

    def rel_degree(self, vertex):
        """ Returns the ratio: indegree / outdegree
        """
        return self.indegree(vertex) / self.outdegree(vertex)

    def max_degree(self):
        return max([self.degree(vertex) for vertex in self.vertices])

    def cleanup(self):
        for v in self.vertices:
            if self.degree(v) == 0:
                self.vertices.remove(v)
                del self.edges[v]

    def componentsWithEdges(self, edgeSet):
        components = []
        to_check = list(self.vertices)
        while len(to_check) > 0:
            v = to_check.pop()
            component = [v]
            added = [v]
            while len(added) > 0:
                vertex = added.pop()
                for edge in edgeSet:
                    if edge[0] == vertex:
                        if edge[1] in to_check:
                            component.append(edge[1])
                            to_check.remove(edge[1])
                            added.append(edge[1])
                    if edge[1] == vertex:
                        if edge[0] in to_check:
                            component.append(edge[0])
                            to_check.remove(edge[0])
                            added.append(edge[0])
            components.append(component)
        return len(components)

    def load_from_file(self, filename='graph.txt'):
        edge_set = []
        with open(filename) as file:
            for line in file:
                v_1, v_2 = line[:-1].split(" ")
                edge_set.append([int(v_1), int(v_2)])
        self.addEdgeSet(edge_set)

    def load_from_list(self, data):
        edge_set = []
        for lines in data:
            for line in lines.split('\n'):
                if not line:
                    continue

                v_1, v_2 = line.split(" ")
                edge_set.append([int(v_1), int(v_2)])

        self.addEdgeSet(edge_set, ignore_duplicates=True)

    def save_to_file(self, filename="graph.txt", mode='adjacency_list', bidirectional=True):
        if mode == 'adjacency_list':
            with open(filename, 'w') as file:
                for vertex_1 in self.vertices:
                    for vertex_2 in self.edges[vertex_1]:
                        if not bidirectional or vertex_1 < vertex_2:
                            file.write('{} {}\n'.format(
                                str(vertex_1), str(vertex_2)))

        else:
            raise NotImplementedError()
        print('done')

    def save_as_pickle(self, filename="graph"):
        with open('{}.pkl'.format(filename), 'wb') as file:
            pickle.dump(self, file)

    def load_pickle(self, filename="graph"):
        with open('{}.pkl'.format(filename), 'rb') as file:
            return pickle.load(file)

    def __str__(self):
        return "Graph |V|=" + str(len(self.vertices)) + ", |E|=" + str(self.n_edges)

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(str(self.edges))


def graph_from_file(filename='graph.txt'):
    edge_set = []
    with open(filename) as file:
        for line in file:
            v_1, v_2 = line[:-1].split(" ")
            edge_set.append([int(v_1), int(v_2)])
    graph = Graph()
    graph.addEdgeSet(edge_set)

    return graph
