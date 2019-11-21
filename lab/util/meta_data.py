class MetaData:
    def __init__(self, worker_id, min_vertex, max_vertex, host=None, port=None):
        self.worker_id = worker_id
        self.min_vertex = min_vertex
        self.max_vertex = max_vertex
        self.host = host
        self.port = port

    def set_connection_info(self, host, port):
        self.host = host
        self.port = port

    def get_connection_info(self):
        return self.host, self.port

    def has_vertex(self, vertex) -> bool:
        return self.min_vertex <= vertex <= self.max_vertex

    def to_dict(self) -> dict:
        return {
            'worker_id': self.worker_id,
            'min_vertex': self.min_vertex,
            'max_vertex': self.max_vertex,
            'host': self.host,
            'port': self.port
        }
