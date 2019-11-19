import sys


def print_error(message):
    """
    Print message as error

    :param message: Message to print
    """

    print(message, file=sys.stderr)
