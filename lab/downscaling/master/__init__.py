from lab.downscaling.util.argument_parser import get_arg
from lab.downscaling.util.output import print_error
from lab.downscaling.util.validation import is_positive_integer, is_path
from lab.downscaling.master.Master import Master


def run(n_workers: int, graph_path: str):
    """
    Starts the master

    :param n_workers: Number of workers to create
    :param graph_path: The path to the graph the downscale
    """

    master = Master(n_workers, graph_path)


def main():
    """
    Parses arguments to be used for the run
    """

    try:
        n_workers = get_arg("--n_workers", is_positive_integer)
        graph_path = get_arg("--graph", is_path)
        run(n_workers, graph_path)
    except Exception as e:
        print_error(e)
        print_error(
            "The downscaling master expects the following arguments:\n"
            "\t--n_workers: The number of workers to create\n"
            "\t--graph: The path to the graph the downscale\n"
        )


if __name__ == '__main__':
    main()
