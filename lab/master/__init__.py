from lab.util.argument_parser import get_arg
from lab.util.output import print_error
from lab.util.validation import assert_bool,  assert_path, assert_file, assert_pos_float, assert_master_type, assert_method, assert_list, assert_positive_int


def main():
    """
    Parses arguments to be used for the run
    """

    try:
        worker_hostnames = get_arg("--worker-hostnames", assert_list)
        graph_path = get_arg("--graph", assert_file)
        master_func = get_arg("--master", assert_master_type, default='Master')
        worker_script = get_arg("--worker-script", assert_file)
        split_graph = get_arg("--split_graph", assert_bool, default=True)
        output_file = get_arg("--output-file", assert_path,
                              default='data/graph_generated.txt')
        scale = get_arg("--scale", assert_pos_float)
        method = get_arg("--method", assert_method)
        backup_size = get_arg("--backup-size", assert_positive_int, default=100)
        walking_iterations = get_arg("--walking-iterations", assert_positive_int, default=1)
        random_walkers_per_worker = get_arg("--random-walkers-per-worker", assert_positive_int, default=1)
        debug = get_arg("--debug", assert_bool, default=True)
    except AssertionError as e:
        print_error(e)
        print_error(
            "The master expects the following arguments:\n"
            "\t--worker-hostnames: The hostnames of the workers separated by commas (without spaces)\n"
            "\t--graph: The path to the graph to scale\n"
            "\t--worker-script: Python script for worker\n"
            "\t--split_graph: Flag to indicate to whether to split up the graph in smaller subgraphs or to copy the whole graph to every worker\n"
            "\t--output-file: File to save the scaled graph in\n"
            "\t--scale: The scale to which the graph should be up/down-sized, should be above zero\n"
            "\t--method: The method used for downscaling, `random_walk` or `random_edge`\n"
            "\t--random-walkers-per-worker: The number of random walker to start per worker\n"
            "\t--backup-size: Minimum size of the backup before it will be send to the master during a run\n"
            "\t--walking-iterations: The number of steps a random walker sets before the queue will be handled\n"
            "\t--debug: Show debug messages"
        )
        return

    # by default, start lab.master.Master.Master
    master_func(worker_hostnames, graph_path, worker_script,
                split_graph, output_file, scale, method,
                random_walkers_per_worker, backup_size,
                walking_iterations, debug)


if __name__ == '__main__':
    main()
