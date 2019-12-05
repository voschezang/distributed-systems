from lab.util.argument_parser import get_arg
from lab.util.output import print_error
from lab.util.validation import (
    assert_nonnegative_int,
    assert_positive_int,
    assert_host,
    assert_file,
    assert_pos_float,
    assert_method, assert_path)
from lab.downscaling.worker.Worker import Worker


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
        output_file = get_arg("--output", assert_path)
    except AssertionError as e:
        print_error(e)
        print_error(
            "The downscaling worker expects the following arguments:\n"
            "\t--worker-id: The id of the worker\n"
            "\t--master-host: The host of the master\n"
            "\t--master-port: The port of the master\n"
            "\t--graph: The path to the graph to downscale\n"
            "\t--scale: The scale of the downscaled graph w.r.t. the input graph\n"
            "\t--method: The method to use for downscaling, `random_walk` or `random_edge`\n"
            "\t--output: File to output the downscaled graph to\n"
        )
        return

    Worker(worker_id, master_host, master_port, graph_path, scale, method, output_file)


if __name__ == '__main__':
    main()
