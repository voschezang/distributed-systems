import sys
from Algorithm import Algorithm, RandomNodeAlgorithm
from Communication import Communication


class Worker:
    def __init__(self, arguments):
        self.arguments = arguments
        self.communication = communication
        self.algorithm = algorithm

    def run(self):
        graph = self.communication.receive_graph()
        return self.algorithm.run()


if __name__ == "__main__":
    w = Worker(sys.argv)
