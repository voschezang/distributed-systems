from lab.util.argument_parser import get_arg
from lab.util.output import print_error
from lab.util.validation import (
    assert_nonnegative_int,
    assert_positive_int,
    assert_host,
    assert_file,
    assert_pos_float,
    assert_method)
from lab.downscaling.worker.Worker import Worker


def run(worker_id: int, master_host: str, master_port: int, graph_path: str, scale: float, method: str):
    """
    Starts the worker

    :param worker_id: The id of the worker
    :param master_host: The host of the master
    :param master_port: The port of the master
    :param graph_path: The path to the graph to downscale
    :param scale: The scale to which the graph should be downsized
    :param method: The method used for downscaling, `random_walk` or `random_edge`
    """

    worker = Worker(worker_id, master_host, master_port, graph_path, scale, method)


def main():
    """
    Parses arguments to be used for the run
    """

    try:
        worker_id = get_arg("--worker-id", assert_nonnegative_int)
        master_host = get_arg("--master-host", assert_host)
        master_port = get_arg("--master-port", assert_positive_int)
        graph_path = get_arg("--graph", assert_file)
        scale = get_arg("--scale", assert_pos_float)
        method = get_arg("--method", assert_method)
    except Exception as e:
        print_error(e)
        print_error(
            "The downscaling worker expects the following arguments:\n"
            "\t--worker-id: The id of the worker\n"
            "\t--master-host: The host of the master\n"
            "\t--master-port: The port of the master\n"
            "\t--graph: The path to the graph to downscale\n"
            "\t--scale: The scale of the downscaled graph w.r.t. the input graph\n"
            "\t--method: The method to use for downscaling, `random_walk` or `random_edge`\n"
        )
        return

    run(worker_id, master_host, master_port, graph_path, scale, method)


if __name__ == '__main__':
    main()
