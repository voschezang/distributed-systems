import sys
from Algorithm import DegreeDistrubutionAlgorithm, RandomNodeAlgorithm
from Communication import Communication


class Worker:
    def __init__(self, arguments, algorithm='DegreeDistrubutionAlgorithm'):
        self.parse_arguments(arguments)
        self.communication = Communication(self.arguments)
        if algorithm == 'DegreeDistrubutionAlgorithm':
            self.algorithm = DegreeDistrubutionAlgorithm

    def parse_arguments(self, arguments):
        decoded_arguments = {}
        for argument in arguments[1:]:
            argument, value = argument.split(':')
            decoded_arguments[argument] = value

        self.arguments = decoded_arguments

    def run(self):
        self.original_graph = self.communication.receive_graph()
        # Dubbele self arguments nodig, nog te debuggen
        self.generated_graph = self.algorithm.run(self, self)
        self.generated_graph.save_to_file('upscaled_graph.txt')


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.argv = ['Worker.py', 'master_server_domain:localhost',
                    'master_server_port:8890', 'scale:2']
    w = Worker(sys.argv)
    print(w)
    w.run()
