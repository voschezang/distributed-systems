from lab.util.graph import Graph
from lab.master.WorkerInterface import WorkerInterface
from lab.upscaling.worker import Algorithm


class Worker(WorkerInterface):
    def run(self):
        graph = Graph()
        graph.load_from_file(self.graph)
        diff = Algorithm.DegreeDistrubution().run(graph)
        prefix, ext = self.graph.split('.')
        filename = f'{prefix}-par-{self.worker_id}{ext}'
        diff.save_to_file(filename)
        print('worker done, id:', self.worker_id)
        self.send_job_complete()
