from lab.util.argument_parser import get_arg
from lab.util.output import print_error
from lab.util.validation import assert_positive_integer, assert_path
from lab.downscaling.master.Master import Master


def run(n_workers: int, graph_path: str, worker_script: str):
    """
    Starts the master

    :param n_workers: Number of workers to create
    :param graph_path: The path to the graph the downscale
    :param worker_script: Python script for worker
    """

    master = Master(n_workers, graph_path, worker_script)


def main():
    """
    Parses arguments to be used for the run
    """

    try:
        n_workers = get_arg("--n_workers", assert_positive_integer)
        graph_path = get_arg("--graph", assert_path)
        worker_script = get_arg("--worker-script", assert_path)
    except Exception as e:
        print_error(e)
        print_error(
            "The downscaling master expects the following arguments:\n"
            "\t--n_workers: The number of workers to create\n"
            "\t--graph: The path to the graph the downscale\n"
            "\t--worker-script: Python script for worker\n"
        )
        return

    run(n_workers, graph_path, worker_script)


if __name__ == '__main__':
    main()
