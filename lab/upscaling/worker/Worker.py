from lab.util.graph import Graph
from lab.util import message
from lab.master.WorkerInterface import WorkerInterface
from lab.upscaling.worker import Algorithm


class Worker(WorkerInterface):
    def __init__(self, worker_id: int, master_host: str, master_port: int, graph_path: str):
        super().__init__(worker_id, master_host, master_port, graph_path)
        self.message_interface = {
            message.FINISH_JOB: self.handle_finish_job
        }
        self.run()

    def send_progress_message(self, step):
        self.send_message_to_master(
            message.write_progress(self.worker_id, step))

    def run(self):
        graph = Graph()
        graph.load_from_file(self.graph_path)
        algorithm = Algorithm.DegreeDistrubution(graph)

        step = 0
        for step, _ in enumerate(algorithm):
            if self.terminate:
                break

            self.handle_queue()
            step += 1

            if step % 100 == 0:
                self.send_progress_message(step)

        diff = algorithm.scaled_graph

        prefix, ext = self.graph_path .split('.')
        filename = f'{prefix}-par-{self.worker_id}{ext}'
        diff.save_to_file(filename)
        print('worker done, id:', self.worker_id)
