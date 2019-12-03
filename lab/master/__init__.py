from lab.util.argument_parser import get_arg
from lab.util.output import print_error
from lab.util.validation import assert_bool, assert_nonnegative_int,  assert_path, assert_file, assert_pos_float, assert_master_type


def main():
    """
    Parses arguments to be used for the run
    """

    try:
        n_workers = get_arg("--n_workers", assert_nonnegative_int)
        graph_path = get_arg("--graph", assert_file)
        master_func = get_arg("--master", assert_master_type, default='Master')
        worker_script = get_arg("--worker-script", assert_file)
        split_graph = get_arg("--split_graph", assert_bool, default=True)
        output_file = get_arg("--output-file", assert_path,
                              default='data/graph_generated.txt')
        scale = get_arg("--scale", assert_pos_float)
    except Exception as e:
        print_error(e)
        print_error(
            "The master expects the following arguments:\n"
            "\t--n_workers: The number of workers to create\n"
            "\t--graph: The path to the graph to scale\n"
            "\t--worker-script: Python script for worker\n"
            "\t--split_graph: Flag to indicate to whether to split up the graph in smaller subgraphs or to copy the whole graph to every worker\n"
            "\t--output-file: File to save the scaled graph in\n"
            "\t--scale: The scale to which the graph should be up/down-sized, should be above zero\n"
        )
        return

    # by default, start lab.master.Master.Master
    master_func(n_workers, graph_path, worker_script,
                split_graph, output_file, scale)


if __name__ == '__main__':
    main()
