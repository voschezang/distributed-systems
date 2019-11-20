import os


def assert_nonnegtive_int(name: str, value: str) -> int:
    """
    Makes sure the value is a non-negative integer, otherwise raises AssertionError

    :param name: Argument name
    :param value: Value
    :return: Value as integer
    """
    if int(value)
    if not (value.isdigit() and int(value) >= 0):
        raise AssertionError(
            "Expected a non-negative integer for {}, but got `{}`".format(name, value))

    return int(value)


def assert_positive_int(name: str, value: str) -> int:
    """
    Makes sure the value is a positive integer, otherwise raises AssertionError

    :param name: Argument name
    :param value: Value
    :return: Value as integer
    """
    if not (value.isdigit() and int(value) > 0):
        raise AssertionError(
            "Expected a positive integer for {}, but got `{}`".format(name, value))

    return int(value)


def assert_path(name: str, value: str) -> str:
    """
    Makes sure the value is a path to an existing file, otherwise raises AssertionError

    :param name: Argument name
    :param value: Value
    :return: Value as string
    """
    if not (os.path.exists(value) and os.path.isfile(value)):
        raise AssertionError("Invalid path for {}: `{}`".format(name, value))

    return value


def assert_host(name: str, value: str) -> str:
    """
    Makes sure the value is an available host, otherwise raises AssertionError

    :param name: Argument name
    :param value: Value
    :return: Value as string
    """
    if os.system("ping -c 1 -w2 {} > /dev/null 2>&1".format(value)) != 0:
        raise AssertionError("Invalid path for {}: `{}`".format(name, value))

    return value


def no_assertion(name: str, value: str) -> str:
    """
    Make no assertion on the value

    :param name: Argument name
    :param value: Value
    :return: Value
    """
    return value
