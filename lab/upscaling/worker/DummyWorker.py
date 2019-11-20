from lab.util.message import ALIVE
import time
from lab.master.WorkerInterface import WorkerInterface


class DummyWorker(WorkerInterface):
    def run(self):
        """
        Runs the worker
        """
        print('no base class')

        while True:
            time.sleep(1)
            self.send_message_to_master(ALIVE, self.worker_id)
