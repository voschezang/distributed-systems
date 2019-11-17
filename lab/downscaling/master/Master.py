from lab.downscaling.util.file_io import read_in_chunks, write_chunk


class Master:
    def __init__(self, n_workers: int, graph_path: str):
        self.n_workers = n_workers
        self.sub_graph_paths = self.divide_graph(graph_path)
        print(self.sub_graph_paths)

    def divide_graph(self, graph_path: str) -> [str]:
        """
        Divides the graph into `number_of_workers` sub graphs and writes each chunk to a separate file

        :param graph_path: Path to the file containing the entire graph
        :return: List of paths to the created chunks
        """
        paths = []

        f = open(graph_path, "r")
        for worker_id, sub_graph in enumerate(read_in_chunks(f, self.n_workers)):
            paths.append(write_chunk(worker_id, sub_graph))
        f.close()

        return paths
