from lab.util import sockets
from lab.util.message import REGISTER, ALIVE
import time
from multiprocessing import Process, Queue
from lab.util.server import Server
from lab.master.WorkerInterface import WorkerInterface


class DummyWorker(WorkerInterface):
    def send_message_to_master(self, status: str, *args):
        """
        Sends a message to the master

        :param status: Status of the message (Should be one mentioned in lab.util.message)
        """

        sockets.send_message(self.master_host, self.master_port, status, *args)

    def register(self):
        """
        Sends a REGISTER request to the master
        """
        self.send_message_to_master(
            REGISTER, self.worker_id, self.hostname, self.port)

    def run(self):
        """
        Runs the worker
        """

        while True:
            time.sleep(1)
            self.send_message_to_master(ALIVE, self.worker_id)
