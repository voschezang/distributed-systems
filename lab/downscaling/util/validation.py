import os


def is_positive_integer(name: str, value: str):
    """
    Makes sure the value is a positive integer, otherwise raises AssertionError

    :param name: Argument name
    :param value: Value name
    :return:
    """
    if not (value.isdigit() and int(value) > 0):
        raise AssertionError("Expected a positive integer for {}, but got `{}`".format(name, value))

    return int(value)


def is_path(name: str, value: str):
    if not (os.path.exists(value) and os.path.isfile(value)):
        raise AssertionError("Invalid path for {}: `{}`".format(name, value))

    return value
