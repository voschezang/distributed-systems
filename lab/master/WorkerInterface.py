from multiprocessing import Process
from time import sleep

from lab.util import sockets, message
from lab.util.meta_data import MetaData, CombinedMetaData
from lab.util.server import Server
from lab.util import message, file_io, validation
from lab.util.file_transfer import FileReceiver, UnexpectedChunkIndex, FileSender
from lab.util.meta_data import CombinedMetaData, MetaData
from typing import Dict


class Client:
    """ Worker node representing a process that may communicate with master and
    other workers, superclass for the actual workers.
    """

    def __init__(self, worker_id: int, master_host: str, master_port: int):
        self.worker_id = worker_id
        self.master_host = master_host
        self.master_port = master_port

    def send_message_to_master(self, message_to_send: bytes):
        """
        Sends a message to the master
        """
        for i in range(100):
            try:
                sockets.send_message(
                    self.master_host, self.master_port, message_to_send)
                return
            except ConnectionResetError:
                # Try again until success
                pass
            except BrokenPipeError:
                # Try again until success
                pass

            sleep(0.01 * i)

        raise Exception(
            f'Failed to connect to master. worker_id: {self.worker_id}')

    @staticmethod
    def send_message_to_node(host, port, message_to_send: bytes):
        sockets.send_message(host, port, message_to_send)


class HeartbeatDaemon(Client):
    """ Daemon process that periodically pings Master to indicate the
    corresponding worker is alive.
    """

    def __init__(self, worker_id: int, master_host: str, master_port: int,
                 wait_time: float):
        super().__init__(worker_id, master_host, master_port)

        while True:
            try:
                self.send_message_to_master(
                    message.write_alive(self.worker_id))
            except ConnectionRefusedError:
                return

            sleep(wait_time)


class WorkerInterface(Client, Server):
    """ Actually an abstract base class
    """

    def __init__(self, worker_id: int, master_host: str, master_port: int):
        super().__init__(worker_id, master_host, master_port)
        self.re_init()  # init Server

        self.cancel = False

        # Register self at master
        self.register()

        # Wait for the meta data of the other workers
        self.combined_meta_data: CombinedMetaData = self.receive_meta_data()

        self.file_receivers: Dict[int, FileReceiver] = {
            message.GRAPH: None,
            message.BACKUP: None
        }

        self.backup_sender = None

        self.init_heartbeat_daemon(wait_time=0.5)

    def run(self):
        raise NotImplementedError()

    def send_progress_message(self, count):
        self.send_message_to_master(
            message.write_progress(self.worker_id, count))

    def handle_finish_job(self, *args):
        self.cancel = True

    def handle_terminate(self):
        self.heartbeat_daemon.terminate()
        self.server.terminate()

    def handle_meta_data(self, all_meta_data):
        self.combined_meta_data = CombinedMetaData([
            MetaData(
                worker_id=meta_data['worker_id'],
                number_of_edges=meta_data['number_of_edges'],
                min_vertex=meta_data['min_vertex'],
                max_vertex=meta_data['max_vertex'],
                host=meta_data['host'],
                port=meta_data['port']
            )
            for meta_data in all_meta_data
        ])

    def receive_meta_data(self) -> CombinedMetaData:
        status, all_meta_data = self.get_message_from_queue()

        return CombinedMetaData([
            MetaData(
                worker_id=meta_data['worker_id'],
                number_of_edges=meta_data['number_of_edges'],
                min_vertex=meta_data['min_vertex'],
                max_vertex=meta_data['max_vertex'],
                host=meta_data['host'],
                port=meta_data['port']
            )
            for meta_data in all_meta_data
        ])

    def handle_start_send_file(self, worker_id, file_type, number_of_chunks):
        self.file_receivers[file_type] = FileReceiver(number_of_chunks)

    def handle_file_chunk(self, worker_id, file_type, index, chunk):
        try:
            self.file_receivers[file_type].receive_chunk(index, chunk)
        except UnexpectedChunkIndex as e:
            self.send_message_to_master(message.write_missing_chunk(
                self.worker_id, file_type, e.expected_index))

    def handle_end_send_file(self, worker_id, file_type):
        try:
            self.file_receivers[file_type].handle_end_send_file()
            self.send_message_to_master(
                message.write_received_file(self.worker_id, file_type))
        except UnexpectedChunkIndex as e:
            self.send_message_to_master(message.write_missing_chunk(
                self.worker_id, file_type, e.expected_index))

    def handle_missing_chunk(self, worker_id, file_type, index):
        self.backup_sender.index = index

    def handle_received_file(self, worker_id, file_type):
        self.backup_sender.target_received_file = True

    def send_backup_to_master(self, data: list):
        self.backup_sender = FileSender(
            self.worker_id, message.BACKUP, data=data)

        self.send_message_to_master(message.write_start_send_file(
            self.worker_id, message.BACKUP, len(self.backup_sender.messages)))
        while not self.backup_sender.target_received_file or not self.backup_sender.complete_file_send:
            if self.backup_sender.complete_file_send:
                self.send_message_to_master(
                    message.write_end_send_file(self.worker_id, message.BACKUP))
                sleep(0.1)
            else:
                self.send_message_to_master(
                    self.backup_sender.get_next_message())

            self.handle_queue()

        self.backup_sender = None

    def receive_graph(self):
        while self.file_receivers[message.GRAPH] is None or not self.file_receivers[message.GRAPH].received_complete_file:
            self.handle_queue()

    def register(self):
        """
        Sends a REGISTER request to the master
        """

        self.send_message_to_master(message.write_register(
            self.worker_id,
            self.hostname,
            self.port
        ))

    def send_job_complete(self):
        """ Sends a JOB_COMPLETE to master
        """

        self.send_message_to_master(
            message.write_job(message.JOB_COMPLETE, self.worker_id))

    def send_debug_message(self, debug_message: str):
        self.send_message_to_master(message.write_debug(
            self.worker_id,
            debug_message
        ))

    def init_heartbeat_daemon(self, wait_time: float = 1.0):
        self.heartbeat_daemon = Process(target=HeartbeatDaemon, args=(
            self.worker_id, self.master_host, self.master_port, wait_time))
        self.heartbeat_daemon.start()
