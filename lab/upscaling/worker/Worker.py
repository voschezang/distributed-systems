from time import sleep

from lab.util.graph import Graph
from lab.util import message
from lab.master.WorkerInterface import WorkerInterface
from lab.upscaling.worker import Algorithm


class Worker(WorkerInterface):
    def __init__(self, worker_id: int, master_host: str, master_port: int, graph_path: str):
        super().__init__(worker_id, master_host, master_port, graph_path)
        self.message_interface = {
            message.FINISH_JOB: self.handle_finish_job,
            message.TERMINATE: self.handle_terminate
        }
        self.run()

    def send_progress_message(self, step):
        self.send_message_to_master(
            message.write_progress(self.worker_id, step))

    @property
    def output_filename(self):
        prefix, ext = self.graph_path.split('.')
        return f'{prefix}-par-{self.worker_id}.{ext}'

    def run(self):
        graph = Graph()
        graph.load_from_file(self.graph_path)
        algorithm = Algorithm.DegreeDistrubution(graph)

        step = 0
        for step, _ in enumerate(algorithm):
            if self.cancel:
                break

            self.handle_queue()
            step += 1

            if step % 100 == 0:
                self.send_progress_message(step)

        diff = algorithm.scaled_graph
        diff.save_to_file(self.output_filename, bidirectional=False)

        self.send_job_complete(self.output_filename)

        while True:
            sleep(0.1)
            self.handle_queue()

        # self.terminate()
