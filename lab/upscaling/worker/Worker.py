import time

from lab.util.graph import Graph
from lab.master.WorkerInterface import WorkerInterface
from lab.util import message
# from lab.upscaling.worker.Algorithm import Algorithm
from lab.upscaling.worker import Algorithm


class Worker(WorkerInterface):
    def run(self):
        for _ in range(2):
            time.sleep(0.)
            self.send_message_to_master(message.write_alive(self.worker_id))

        graph = Graph()
        graph.load_from_file(self.graph)
        diff = Algorithm.DegreeDistrubution().run(graph)
        # TODO send confirmation to master, then ask for permission to write
        filename = f'{self.graph}-par-{self.worker_id}.txt'
        diff.save_to_file(filename)
