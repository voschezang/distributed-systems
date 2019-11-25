class MetaData:
    def __init__(self, worker_id: int, number_of_edges: int, min_vertex: int, max_vertex: int, host: str = None, port: str = None):
        self.worker_id = worker_id
        self.number_of_edges = number_of_edges
        self.min_vertex = min_vertex
        self.max_vertex = max_vertex
        self.host = host
        self.port = port

    def set_connection_info(self, host, port):
        self.host = host
        self.port = port

    def get_connection_info(self):
        return self.host, self.port

    def has_vertex(self, vertex: int) -> bool:
        print(self.min_vertex, vertex, self.max_vertex)
        return self.min_vertex <= vertex <= self.max_vertex

    def to_dict(self) -> dict:
        return {
            'worker_id': self.worker_id,
            'number_of_edges': self.number_of_edges,
            'min_vertex': self.min_vertex,
            'max_vertex': self.max_vertex,
            'host': self.host,
            'port': self.port
        }


class CombinedMetaData:
    def __init__(self, combined_meta_data: [MetaData]):
        self.combined_meta_data = combined_meta_data
        self.top_layer = self.find_top_layer
        self.bottom_layer = self.find_bottom_layer
        self.combined_number_of_edges = self.get_combined_number_of_edges

    @property
    def find_top_layer(self) -> MetaData:
        top_layer = None
        max_vertex = None

        for meta_data in self.combined_meta_data:
            if max_vertex is None or meta_data.max_vertex > max_vertex:
                top_layer = meta_data
                max_vertex = meta_data.max_vertex

        return top_layer

    @property
    def find_bottom_layer(self) -> MetaData:
        bottom_layer = None
        min_vertex = None

        for meta_data in self.combined_meta_data:
            if min_vertex is None or meta_data.min_vertex < min_vertex:
                bottom_layer = meta_data
                min_vertex = meta_data.min_vertex

        return bottom_layer

    @property
    def get_combined_number_of_edges(self):
        return sum([meta_data.number_of_edges for meta_data in self.combined_meta_data])

    def get_worker_id_that_has_vertex(self, vertex: int):
        for meta_data in self.combined_meta_data:
            if meta_data.has_vertex(vertex):
                return meta_data.worker_id

        raise Exception("Vertex could not be matched to any of the workers")

    def get_connection_that_has_vertex(self, vertex: int):
        for meta_data in self.combined_meta_data:
            if meta_data.has_vertex(vertex):
                return meta_data.get_connection_info()

        raise Exception("Vertex could not be matched to any of the workers")
