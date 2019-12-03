from lab.master.Master import Master
from lab.util import message
from time import sleep


class Upscaler(Master):
    def run(self):
        print('Run upscaler')
        # while self.total_progress() < self.goal_size:
        for i in range(10):
            sleep(0.1)
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
        graph = self.create_graph()
        # append original graph
        graph.load_from_file(self.graph_path)
        print(graph)
        self.output_file = 'data/generated_graph.txt'
        graph.write_to_file(self.output_file)
