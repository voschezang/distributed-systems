import pickle


class Graph:
    def __init__(self):
        self.vertices_dict = {}
        self.vertices = []
        self.edges = dict()
        self.amountOfEdges = 0
        self.rawEdges = []
        self.id = 0

    def addEdgeSet(self, edgeSet):
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
            self.addEdge(vertex_1, vertex_2)

    def addEdge(self, vertex_1, vertex_2):
        if not vertex_1 in self.vertices:
            raise ValueError("Vertex %s not known in this graph" % vertex_1)
        if not vertex_2 in self.vertices:
            raise ValueError("Vertex %s not known in this graph" % vertex_2)
        if vertex_2 in self.edges[vertex_1]:
            raise ValueError(
                "Edge (%s, %s) already exists in this graph" % (vertex_1, vertex_2))
        e = Edge(vertex_1, vertex_2)
        self.edges[vertex_1].append(vertex_2)
        if vertex_1 != vertex_2:
            self.edges[vertex_2].append(vertex_1)
        self.rawEdges.append(e)
        self.amountOfEdges += 1
        return e

    def addVertex(self, label):
        v = Vertex(label)
        self.vertices.append(v)
        self.vertices_dict[label] = v
        self.edges[v] = []
        return v

    def copy(self):
        H = Graph()
        H.vertices = list(self.vertices)
        H.edges = dict((v, list(self.edges[v])) for v in self.edges)
        H.amountOfEdges = int(self.amountOfEdges)
        H.id = int(self.id) + 1
        return H

    def degree(self, vertex):
        return len(self.edges[vertex])

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

    def save_to_file(self, filename="graph.txt", mode='adjacency_list'):
        if mode == 'adjacency_list':
            with open(filename, 'w') as file:
                for vertex_1 in self.vertices:
                    for vertex_2 in self.edges[vertex_1]:
                        if vertex_1.label < vertex_2.label:
                            file.write('{} {}\n'.format(
                                vertex_1.label, vertex_2.label))

        print('done')

    def save_as_pickle(self, filename="graph"):
        with open('{}.pkl'.format(filename), 'wb') as file:
            pickle.dump(self, file)

    def load_pickle(self, filename="graph"):
        with open('{}.pkl'.format(filename), 'rb') as file:
            return pickle.load(file)

    def __str__(self):
        return "Graph |V|=" + str(len(self.vertices)) + ", |E|=" + str(self.amountOfEdges)

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(str(self.edges))


class Vertex:
    def __init__(self, label):
        self.label = label

    def __str__(self):
        return str(self.label)

    def __repr__(self):
        return "Vertex " + str(self)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if type(other) == int:
            return self.label == other
        return self.label == other.label


class Edge:
    def __init__(self, vertex_1, vertex_2):
        self.vertex_1 = vertex_1
        self.vertex_2 = vertex_2

    def __str__(self):
        return str(self.vertex_1) + ' - ' + str(self.vertex_2)

    def __repr__(self):
        return str(self)


def graph_from_file(filename='graph.txt'):
    edge_set = []
    with open(filename) as file:
        for line in file:
            v_1, v_2 = line[:-1].split(" ")
            edge_set.append([int(v_1), int(v_2)])
    graph = Graph()
    graph.addEdgeSet(edge_set)

    return graph
