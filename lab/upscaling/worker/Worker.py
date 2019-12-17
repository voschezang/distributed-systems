from time import sleep

from lab.util.graph import Graph
from lab.util import message
from lab.master.WorkerInterface import WorkerInterface
from lab.upscaling.worker import Algorithm


class Worker(WorkerInterface):
    def __init__(self, worker_id: int, master_host: str, master_port: int):
        super().__init__(worker_id, master_host, master_port)
        self.message_interface = {
            message.META_DATA: self.handle_meta_data,
            message.FINISH_JOB: self.handle_finish_job,
            # message.WORKER_FAILED: self.handle_worker_failed,
            message.CONTINUE: self.handle_continue,
            message.START_SEND_FILE: self.handle_start_send_file,
            message.FILE_CHUNK: self.handle_file_chunk,
            message.END_SEND_FILE: self.handle_end_send_file,
            message.MISSING_CHUNK: self.handle_missing_chunk,
            message.RECEIVED_FILE: self.handle_received_file,
            message.TERMINATE: self.handle_terminate
        }
        self.graph_path = 'TODO.txt'
        self.receive_graph()
        self.run()

    # @property
    # def graph_path(self):
    #     print(self.file_receivers[message.GRAPH].file)
    #     return self.file_receivers[message.GRAPH].file

    # def send_progress_message(self, step):
    #     self.send_message_to_master(
    #         message.write_progress(self.worker_id, step))

    @property
    def output_filename(self):
        prefix, ext = self.graph_path.split('.')
        return f'{prefix}-par-{self.worker_id}.{ext}'

    def handle_continue(self):
        pass

    def run(self):
        graph = Graph()
        graph.load_from_list(self.file_receivers[message.GRAPH].file)
        algorithm = Algorithm.DegreeDistrubution(graph)

        step = 0
        for step, _ in enumerate(algorithm):
            if self.cancel:
                break

            self.handle_queue()
            step += 1

            if step % 100 == 0:
                # print(f'worker step {step}')
                self.send_progress_message(step)

        diff = algorithm.scaled_graph
        diff.save_to_file(self.output_filename, bidirectional=False)

        self.send_job_complete()

        while True:
            sleep(0.1)
            self.handle_queue()

        # self.terminate()
