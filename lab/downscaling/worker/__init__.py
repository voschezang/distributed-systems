from lab.util.argument_parser import get_arg
from lab.util.output import print_error
from lab.util.validation import (
    assert_nonnegative_int,
    assert_positive_int,
    assert_host,
    assert_bool,
    assert_pos_float,
    assert_downscaling_method)
from lab.downscaling.worker.Worker import Worker


def main():
    """
    Parses arguments to be used for the run
    """

    try:
        worker_id = get_arg("--worker-id", assert_nonnegative_int)
        master_host = get_arg("--master-host", assert_host)
        master_port = get_arg("--master-port", assert_positive_int)
        scale = get_arg("--scale", assert_pos_float)
        method = get_arg("--method", assert_downscaling_method)
        load_backup = get_arg("--load-backup", assert_bool, default=0)
        number_of_random_walkers = get_arg(
            "--n-random-walkers", assert_nonnegative_int, default=1)
        backup_size = get_arg(
            "--backup-size", assert_nonnegative_int, default=100)
        walking_iterations = get_arg(
            "--walking-iterations", assert_positive_int, default=1)

    except AssertionError as e:
        print_error(e)
        print_error(
            "The downscaling worker expects the following arguments:\n"
            "\t--worker-id: The id of the worker\n"
            "\t--master-host: The host of the master\n"
            "\t--master-port: The port of the master\n"
            "\t--scale: The scale of the downscaled graph w.r.t. the input graph\n"
            "\t--method: The method to use for downscaling, `random_walk` or `random_edge`\n"
            "\t--load-backup: Should the worker load from a backup send by the master\n"
            "\t--n-random-walkers: Number of random walkers to start with\n"
            "\t--backup-size: Minimum size of the backup before it will be send to the master during a run, 0 if you want no backups\n"
            "\t--walking-iterations: The number of steps a random walker sets before the queue will be handled\n"
        )
        return

    Worker(worker_id, master_host, master_port, scale, method, load_backup,
           number_of_random_walkers, backup_size, walking_iterations)


if __name__ == '__main__':
    main()
