import sys
import os


def print_error(message):
    """
    Print message as error

    :param message: Message to print
    """

    print(message, file=sys.stderr)


def get_arg(name: str, assertion):
    try:
        index = sys.argv.index(name)
        value = sys.argv[index + 1]
    except ValueError:
        raise NameError("Unable to find argument: {}".format(name))
    except IndexError:
        raise IndexError("No value found for argument: {}".format(name))

    assertion(name, value)
    return value


def is_positive_integer(name, value):
    if not (value.isDigit() and value > 0):
        raise AssertionError("Expected a positive integer for {}, but got `{}`".format(name, value))


def is_path(name, value):
    if not (isinstance(value, str) and os.path.exists(value) and os.path.isfile(value)):
        raise AssertionError("Invalid path for {}: `{}`".format(name, value))


if __name__ == '__main__':
    try:
        n_workers = get_arg("--n_workers", is_positive_integer)
        graph_path = get_arg("--graph", is_path)
    except Exception as e:
        print_error(e)
        print_error(
            "The downscaling master expects the following arguments:\n"
            "\t--n_workers: The number of worker nodes to create\n"
            "\t--graph: The path to the graph the downscale\n"
        )
