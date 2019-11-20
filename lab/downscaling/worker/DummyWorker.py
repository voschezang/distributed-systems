from lab.util import sockets
from lab.util.message import REGISTER, ALIVE
import time
from multiprocessing import Process, Queue
from lab.util.server import Server
from lab.master.WorkerInterface import WorkerInterface


class DummyWorker(WorkerInterface):
    def run(self):
        """
        Runs the worker
        """

        while True:
            time.sleep(1)
            self.send_message_to_master(ALIVE, self.worker_id)
