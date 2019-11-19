import numpy as np
import socket
import time
from multiprocessing import Process

# from lab.proof_of_concept import Graph


JOB_PREFIX = 'job '


class Master:
    def __init__(self, path='data/facebook_head.txt', port=5000):
        # self.g = Graph()
        # self.g.load_from_file(path)
        self.result = None
        self.port = port
        self.host = socket.gethostname()

        self.init_workers()
        self.init_heartbeat_daemon()

    def run(self):
        data = np.arange(9)  # TODO use self.g
        self.broadcast('1'.encode())
        results = self.receive_results()
        assert len(results) == self.n_workers
        self.combine_worker_results(results)

    def init_workers(self, n_workers=2):
        self.worker_ports = self.port + np.arange(1, n_workers + 1)

        # TODO use Processes (if applicable for DAS)
        self.worker_processes = []
        for port in self.worker_ports:
            worker_process = Process(target=TestWorker, args=[
                                     port, self.port, self.host])
            worker_process.start()
            self.worker_processes.append(worker_process)

        # give workers time to init
        # TODO receive init_done msg
        time.sleep(1)

    def init_heartbeat_daemon(self):
        # TODO start daemon thread
        pass

    @property
    def n_workers(self):
        return len(self.worker_ports)

    def broadcast(self, data):
        for port in self.worker_ports:
            send_msg(self.host, port, data)

    def receive_results(self):
        results = []
        server_socket = init_server(self.port)
        while True:
            if len(results) == self.n_workers:
                break
            else:
                print('results: %i/%i' % (len(results), self.n_workers))

            # TODO allow for more (inf many) connections
            connection, _address = server_socket.accept()
            # TODO allow for longer msgs
            response = connection.recv(1024).decode()
            if response:
                worker_id, job, job_result = decode_job_result(response)
                results.append(job_result)

            connection.close()

        print('done')
        return results

    def combine_worker_results(self, results):
        # TODO
        self.result = None


class TestWorker:
    def __init__(self, worker_id: int, master_port: int, host):
        print('worker init', worker_id)
        server_socket = init_server(worker_id)
        connection, address = server_socket.accept()
        host, port = address  # port is diff from worker_id port
        while True:
            time.sleep(0.1)
            data = connection.recv(1024).decode()  # TODO allow for longer msgs
            if data:
                print('worker', worker_id, 'computing', data)
                time.sleep(0.5 + np.random.random())
                try:
                    job = 0
                    job_result = '0'
                    msg = encode_job_result(worker_id, job, job_result)
                    send_msg(host, master_port, msg)
                except ConnectionRefusedError as e:
                    print('worker', worker_id, e)
                break


def send_msg(host, port: int, msg: bytes):
    assert len(msg) < 1000
    assert isinstance(msg, bytes)
    client_socket = socket.socket()
    client_socket.connect((host, port))
    client_socket.send(msg)
    client_socket.close()


def init_server(port):
    print('init server, port: %i' % port)
    host = socket.gethostname()
    # port = 5000  # must be > 1024
    server_socket = socket.socket()
    address = (host, port)
    server_socket.bind(address)
    queue_length = 5
    server_socket.listen(queue_length)
    return server_socket

# Communication


def encode_job(job: int):
    return (JOB_PREFIX + str(job)).encode()


def decode_job(msg: bytes):
    return msg.decode()[len(JOB_PREFIX):]


def is_job_msg(msg: bytes):
    return msg[:len(JOB_PREFIX)] == JOB_PREFIX


def encode_job_result(*args):
    return '-'.join([str(x) for x in args]).encode()


def decode_job_result(msg):
    msgs = msg.split('-')
    return msgs


if __name__ == "__main__":
    Master().run()
