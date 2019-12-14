from lab.util.argument_parser import get_arg
from lab.util.output import print_error
from lab.util.validation import assert_bool,  assert_path, assert_file, assert_pos_float, assert_master_type, assert_method, assert_list


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
    except Exception as e:
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
        )
        return

    # by default, start lab.master.Master.Master
    master_func(worker_hostnames, graph_path, worker_script,
                split_graph, output_file, scale, method)


if __name__ == '__main__':
    main()
