from typing import Dict
from lab.util.meta_data import MetaData, CombinedMetaData
from time import time
from lab.util.file_io import get_number_of_lines, get_first_line, get_last_line, get_start_vertex, sort_file
from lab.util.command_line import setup_worker

MAX_HEARTBEAT_DELAY = 2.0


class WorkerInfo:
    def __init__(self, worker_id: int, input_sub_graph_path: str, output_sub_graph_path: str, meta_data: MetaData):
        self.worker_id = worker_id
        self.input_sub_graph_path = input_sub_graph_path
        self.output_sub_graph_path = output_sub_graph_path
        self.meta_data = meta_data
        self.process = None
        self.last_alive = None
        self.progress = 0
        self.job_complete = False

    def is_alive(self):
        return self.last_alive is not None and time() - self.last_alive < MAX_HEARTBEAT_DELAY

    def update_meta_data(self):
        self.meta_data.number_of_edges = get_number_of_lines(self.input_sub_graph_path)
        self.meta_data.min_vertex = get_start_vertex(get_first_line(self.input_sub_graph_path))
        self.meta_data.max_vertex = get_start_vertex(get_last_line(self.input_sub_graph_path))

    def sort_sub_graph(self):
        sort_file(self.input_sub_graph_path)

    def terminate(self):
        if self.process is not None:
            self.process.terminate()

    def start_worker(self, worker_script, hostname, port, scale, method):
        self.process = setup_worker(
            worker_script,
            self.worker_id,
            hostname,
            port,
            self.input_sub_graph_path,
            scale,
            method,
            self.output_sub_graph_path
        )


class WorkerInfoCollection:
    def __init__(self):
        self.worker_info_collection: Dict[int, WorkerInfo] = {}

    def __getitem__(self, worker_id: int) -> WorkerInfo:
        return self.worker_info_collection[worker_id]

    def __setitem__(self, worker_id: int, worker_info: WorkerInfo):
        self.worker_info_collection[worker_id] = worker_info

    def keys(self):
        return self.worker_info_collection.keys()

    def items(self):
        return self.worker_info_collection.items()

    def values(self):
        return self.worker_info_collection.values()

    def __len__(self):
        return len(self.worker_info_collection)

    def get_combined_meta_data(self):
        return CombinedMetaData([worker_info.meta_data for worker_info in self.worker_info_collection.values()])

    def get_total_number_of_edges(self):
        return sum([get_number_of_lines(worker_info.input_sub_graph_path) for worker_info in self.worker_info_collection.values()])

    def update_meta_data(self):
        for worker_info in self.worker_info_collection.values():
            worker_info.update_meta_data()

    def sort_sub_graphs(self):
        for worker_info in self.worker_info_collection.values():
            worker_info.sort_sub_graph()

    def terminate_workers(self):
        for worker_info in self.worker_info_collection.values():
            worker_info.process.terminate()

    def get_progress(self):
        return sum([worker_info.progress for worker_info in self.worker_info_collection.values()])

    def all_workers_done(self):
        return all([worker_info.job_complete for worker_info in self.worker_info_collection.values()])

    def start_workers(self, worker_script: str, hostname: str, port: int, scale: float, method: str):
        for worker_info in self.worker_info_collection.values():
            worker_info.start_worker(worker_script, hostname, port, scale, method)