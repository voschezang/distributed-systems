from time import sleep

from lab.util.graph import Graph
from lab.util import message
from lab.master.WorkerInterface import WorkerInterface
from lab.upscaling.worker import Algorithm
from lab.upscaling.worker.Algorithm import GScalerAlgorithm

# You should create this file yourself in order to run the program using ssh
# by default, let shared_filesystem = 0
import lab.util.ssh_connection_info
from lab.util.ssh_connection_info import shared_filesystem

OUTPUT_DIR = 'tmp/upscaling/'


class Worker(WorkerInterface):
    def __init__(self, worker_id: int, master_host: str, master_port: int,
                 method: str = 'DegreeDistribution'):
        print('init')
        super().__init__(worker_id, master_host, master_port)
        self.method = method
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
        if not shared_filesystem:
            self.receive_graph()
        self.run()

    @property
    def output_filename(self):
        return f'{OUTPUT_DIR}scaled_graph_part-{self.worker_id}.txt'

    def handle_continue(self):
        pass

    def run(self):
        print('run w')
        graph = Graph()
        if shared_filesystem:
            graph.load_from_file(lab.util.ssh_connection_info.graph_path)
        else:
            graph.load_from_list(self.file_receivers[message.GRAPH].file)
        algorithm = Algorithm.DegreeDistribution(graph)

        if self.method == 'Gscaler':
            gscaler = GScalerAlgorithm(graph, scale=1.1)
            scaled_graph = gscaler.run()
            scaled_graph.subtract(graph)
            diff = scaled_graph
            self.handle_queue()
            self.send_progress_message(100)
        else:
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
