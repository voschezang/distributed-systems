from time import sleep
import os

from lab.master.Master import Master
from lab.util import message
from lab.upscaling.worker.Worker import OUTPUT_DIR
from lab.util.distributed_graph import DistributedGraph


class Upscaler(Master):
    def run(self):
        print('Upscaler')
        os.system(f'rm -rf {OUTPUT_DIR}')
        os.system(f'mkdir -p {OUTPUT_DIR}')
        # self.broadcast(message.write_continue())
        while self.total_progress() < self.goal_size:
            # for i in range(100):
            sleep(0.01)
            self.handle_queue()
            self.print_progress()
        print("\nJob complete")

        self.broadcast(message.write_job(message.FINISH_JOB))
        # TODO OSError is raised here occacionally
        self.wait_for_workers_to_complete()
        self.terminate_workers()
        self.server.terminate()

        print('Aggregate graph')
        # aggregate generated edges
        graph = DistributedGraph(distributed=False)
        for f in os.listdir(OUTPUT_DIR):
            graph.load_from_file(OUTPUT_DIR + f)
        print(graph)
        # append original graph
        graph.load_from_file(self.graph_path)
        print(graph)
        graph.write_to_file(self.output_file)
