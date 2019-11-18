import numpy as np


class Algorithm:
    def __init__(self):
        print('Algorithm init')

    def run(self):
        raise NotImplementedError


class RandomNodeAlgorithm(Algorithm):
    def run(self):
        print('run')


class DegreeDistrubutionAlgorithm(Algorithm):
    def run(self, worker):
        old_size = len(worker.original_graph.vertices)
        new_size = old_size * int(worker.arguments['scale'])
        print('starting algorithm. Old size: {} New size: {}'.format(
            old_size, new_size))
        degree_count = []
        for vertex in worker.original_graph.vertices:
            degree = worker.original_graph.degree(vertex)
            while degree >= len(degree_count):
                degree_count.append(0)
            degree_count[degree] += 1

        max_degree = len(degree_count)
        all_degrees = list(range(max_degree))
        number_of_degrees = sum(degree_count)
        new_vertex_label = old_size

        new_graph = worker.original_graph.copy()
        for _ in range(new_size - old_size):
            degree_distribution = [d / number_of_degrees for d in degree_count]
            try:
                new_vertex_degree = np.random.choice(
                    all_degrees, p=degree_distribution)
            except:
                print('negative probability')
                print('old/current/new', old_size, old_size+_, new_size)
                print(degree_count)
                print(degree_distribution)
                # Hack a little bit, make the sum of probabilities 1
                degree_distribution_fix = [
                    max(d / number_of_degrees, 0) for d in degree_count]
                lost = 1 - sum(degree_distribution_fix)
                print("lost", lost)
                random_degree = np.random.choice(all_degrees)
                sum(degree_distribution_fix)
                degree_distribution_fix[random_degree] += lost
                sum(degree_distribution_fix)
                new_vertex_degree = np.random.choice(
                    all_degrees, p=degree_distribution_fix)
            print('adding node with degree {}'.format(new_vertex_degree))

            new_neighbours = np.random.choice(
                new_graph.vertices, size=new_vertex_degree, replace=False)

            new_vertex = new_graph.addVertex(new_vertex_label)
            for new_neighbour in new_neighbours:
                new_graph.addEdge(new_vertex, new_neighbour)
                degree_count[new_vertex_degree] += 1
                neighbour_new_degree = new_graph.degree(new_neighbour)
                degree_count[neighbour_new_degree - 1] -= 1
                while neighbour_new_degree >= len(degree_count):
                    degree_count.append(0)
                    all_degrees.append(len(all_degrees))
                    max_degree += 1
                degree_count[neighbour_new_degree] += 1

            new_vertex_label += 1
            number_of_degrees += new_vertex_degree

        return new_graph
