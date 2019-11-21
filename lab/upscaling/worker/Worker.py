import sys
import time

from lab.util.graph import Graph
from lab.master.WorkerInterface import WorkerInterface


class DummyWorker(WorkerInterface):
    def run(self):
        """
        Runs the worker
        """

        while True:
            time.sleep(1)
            self.send_message_to_master(ALIVE, self.worker_id)


class Worker(WorkerInterface):
    # def __init__(self, arguments, algorithm: Algorithm):
    #     self.parse_arguments(arguments)
    #     # self.communication = Communication(self.arguments)
    #     self.algorithm = algorithm

    def run(self):
        # self.original_graph = self.communication.receive_graph()
        # Dubbele self arguments nodig, nog te debuggen
        # self.generated_graph = self.algorithm.run(self, self)
        # self.generated_graph.save_to_file('upscaled_graph.txt')
        graph = Graph(self.graph)
        diff = Algorithm.DegreeDistrubution(graph)

    def parse_arguments(self, arguments):
        decoded_arguments = {}
        for argument in arguments[1:]:
            argument, value = argument.split(':')
            decoded_arguments[argument] = value

        self.arguments = decoded_arguments


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.argv = ['Worker.py', 'master_server_domain:localhost',
                    'master_server_port:8890', 'scale:2']
    w = Worker(sys.argv)
    print(w)
    w.run()
