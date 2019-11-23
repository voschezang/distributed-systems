from lab.util.file_io import get_start_vertex, get_first_line, get_last_line


class MetaData:
    def __init__(self, worker_id, sub_graph_path, host=None, port=None):
        self.worker_id = worker_id
        self.sub_graph_path = sub_graph_path,
        self.min_vertex = get_start_vertex(get_first_line(sub_graph_path))
        self.max_vertex = get_start_vertex(get_last_line(sub_graph_path))
        self.host = host
        self.port = port

    def set_connection_info(self, host, port):
        self.host = host
        self.port = port

    def get_connection_info(self):
        return self.host, self.port

    def has_vertex(self, vertex: int) -> bool:
        return self.min_vertex <= vertex <= self.max_vertex

    def to_dict(self) -> dict:
        return {
            'worker_id': self.worker_id,
            'min_vertex': self.min_vertex,
            'max_vertex': self.max_vertex,
            'host': self.host,
            'port': self.port
        }


def get_connection_info_of_worker_with_vertex(all_meta_data: [MetaData], vertex: int):
    for meta_data in all_meta_data:
        if meta_data.has_vertex(vertex):
            return meta_data.get_connection_info()


def get_path_of_worker_with_vertex(all_meta_data: [MetaData], vertex: int):
    for meta_data in all_meta_data:
        if meta_data.has_vertex(vertex):
            return meta_data.sub_graph_path