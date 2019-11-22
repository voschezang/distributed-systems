from lab.util.argument_parser import get_arg
from lab.util.output import print_error
from lab.util.validation import (
    assert_nonnegative_int,
    assert_positive_int,
    assert_host,
    assert_path)
from lab.upscaling.worker.Worker import Worker
from lab.upscaling.worker.Algorithm import Algorithm


def run(worker_id: int, master_host: str, master_port: int, graph_path: str):
    """
    Starts the worker

    :param worker_id: The id of the worker
    :param master_host: The host of the master
    :param master_port: The port of the master
    :param graph_path: The path to the graph to scale
    """

    worker = Worker(worker_id, master_host, master_port, graph_path)


def main():
    """
    Parses arguments to be used for the run
    """

    master_host = '127.0.0.1'
    try:
        worker_id = get_arg("--worker-id", assert_nonnegative_int)
        # master_host = get_arg("--master-host", assert_host)
        master_port = get_arg("--master-port", assert_positive_int)
        graph_path = get_arg("--graph", assert_path)
    except Exception as e:
        print_error(e)
        print_error(
            "The worker expects the following arguments:\n"
            "\t--worker-id: The id of the worker\n"
            "\t--master-host: The host of the master\n"
            "\t--master-port: The port of the master\n"
            "\t--graph: The path to the graph to scale\n"
        )
        return

    run(worker_id, master_host, master_port, graph_path)


if __name__ == '__main__':
    main()
