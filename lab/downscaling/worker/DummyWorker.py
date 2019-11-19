from lab.util import sockets


class DummyWorker:
    def __init__(self, worker_id: int, master_host: str, master_port: int, graph_path: str):
        self.worker_id = worker_id
        self.master_host = master_host
        self.master_port = master_port
        self.graph = graph_path
        self.master_socket = sockets.connect(master_host, master_port)
        self.socket = sockets.bind("", 0)
        self.hostname = sockets.get_hostname()
        self.port = sockets.get_port(self.socket)
        self.send_alive()

    def send_alive(self):
        self.master_socket.send("{}, {}, {}".format(self.worker_id, self.hostname, self.port).encode())
