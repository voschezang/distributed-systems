from lab.util.argument_parser import get_arg
from lab.util.output import print_error
from lab.util.validation import (
    assert_nonnegative_int,
    assert_positive_int,
    assert_file)
from lab.upscaling.worker.Worker import Worker
from lab.upscaling.worker.Algorithm import Algorithm


def main():
    """
    Parses arguments to be used for the run
    """

    master_host = '127.0.0.1'
    try:
        worker_id = get_arg("--worker-id", assert_nonnegative_int)
        # master_host = get_arg("--master-host", assert_host)
        master_port = get_arg("--master-port", assert_positive_int)
        # graph_path = get_arg("--graph", assert_file)
    except Exception as e:
        print_error(e)
        print_error(
            "The worker expects the following arguments:\n"
            "\t--worker-id: The id of the worker\n"
            "\t--master-host: The host of the master\n"
            "\t--master-port: The port of the master\n"
        )
        return

    Worker(worker_id, master_host, master_port)


if __name__ == '__main__':
    main()
