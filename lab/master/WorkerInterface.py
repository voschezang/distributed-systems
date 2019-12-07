from multiprocessing import Process
from time import sleep

from lab.util import sockets, message
from lab.util.meta_data import MetaData, CombinedMetaData
from lab.util.server import Server


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
                # Possibly: [Errno 54] Connection reset by peer
                # Try again until success
                pass
            except BrokenPipeError:
                # Possibly: [Errno 32] Broken pipe
                pass
            # except Exception as e:
            #     print('Send msg to master error \n\t', e)
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
                self.send_message_to_master(message.write_alive(self.worker_id))
            except ConnectionRefusedError:
                return

            sleep(wait_time)


class WorkerInterface(Client, Server):
    """ Actually an abstract base class
    """

    def __init__(self, worker_id: int, master_host: str, master_port: int,
                 graph_path: str):
        super().__init__(worker_id, master_host, master_port)
        self.re_init()  # init Server

        self.graph_path = graph_path
        self.cancel = False

        # Register self at master
        self.register()

        # Wait for the meta data of the other workers
        self.combined_meta_data: CombinedMetaData = self.receive_meta_data()

        self.init_heartbeat_daemon(wait_time=0.5)

    def run(self):
        raise NotImplementedError()

    def send_progress_message(self, *args):
        raise NotImplementedError()

    def handle_finish_job(self, *args):
        self.cancel = True

    def handle_terminate(self):
        self.heartbeat_daemon.terminate()
        self.server.terminate()

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
