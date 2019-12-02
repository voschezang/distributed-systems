from lab.master import Master
from lab.util import message
from time import sleep


class Master(Master):
    def run(self):
        while self.total_progress() < self.goal_size:
            sleep(0.1)
            self.handle_queue()
            self.print_progress()
        print("\nJob complete")

        self.send_message_to_all_workers(message.write_job_complete())
        self.wait_for_workers_to_complete()
        print('terminate workers')
        self.terminate_workers()
        print('terminate master')
        self.server.terminate()

        print('create d-graph')
        graph = self.create_graph()
        # load original
        print('load or')
        graph.load_from_file(self.graph_path)
        print('gr')
        print(graph)
        graph.write_to_file(self.output_file)
