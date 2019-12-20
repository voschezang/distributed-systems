import os


def assert_list(name: str, value: str) -> list:
    items = value.split(",")

    for item in items:
        if len(item) == 0:
            raise AssertionError(f"Invalid hostname for {name}: `{item}`")

    return items


def assert_bool(name: str, value: str) -> int:
    """
    Makes sure the value is a integer that represents a boolean, otherwise
    raises AssertionError

    :param name: Argument name
    :param value: Value
    :return: Value as integer
    """
    if int(value) not in [0, 1]:
        raise AssertionError(
            "Expected 0 or 1 for {}, but got `{}`".format(name, value))

    return int(value) == 1


def assert_nonnegative_int(name: str, value: str) -> int:
    """
    Makes sure the value is a non-negative integer, otherwise raises AssertionError

    :param name: Argument name
    :param value: Value
    :return: Value as integer
    """
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
    try:
        int_value = int(value)
    except ValueError:
        raise AssertionError(
            "Expected a positive integer for {}, but got `{}`".format(name, value))

    if int_value < 1:
        AssertionError(
            "Expected a positive integer for {}, but got `{}`".format(name, value))

    return int_value


def assert_pos_float(name: str, value: str) -> float:
    """
    Makes sure the value is a positive float, otherwise raises AssertionError

    :param name: Argument name
    :param value: Value
    :return: Value as float
    """
    try:
        value = float(value)
    except ValueError:
        raise AssertionError(f"Invalid value for {name}: `{value}`")

    if value <= 0.0:
        raise AssertionError(
            f"The value should be between above zero, for {name}: `{value}`")

    return value


def assert_file(name: str, value: str) -> str:
    """
    Makes sure the value is a path to an existing file, otherwise raises AssertionError

    :param name: Argument name
    :param value: Value
    :return: Value as string
    """
    if not (os.path.exists(value) and os.path.isfile(value)):
        raise AssertionError("Invalid file for {}: `{}`".format(name, value))

    return value


def assert_path(name: str, value: str) -> str:
    """
    Makes sure the value is a path to an existing file, otherwise raises AssertionError

    :param name: Argument name
    :param value: Value
    :return: Value as string
    """
    end_path_index = value.rindex("/")

    if not os.path.exists(value[:end_path_index]):
        raise AssertionError("Invalid path for {}: `{}`".format(name, value))

    return value


def assert_method(name: str, value: str) -> str:
    if value in ["random_walk", "random_edge", "Gscaler", "DegreeDistribution"]:
        return value
    else:
        raise AssertionError(
            "Invalid scaling method for {}: `{}`".format(name, value))


def assert_downscaling_method(name: str, value: str) -> str:
    if value in ["random_walk", "random_edge"]:
        return value
    else:
        raise AssertionError(
            "Invalid downscaling method for {}: `{}`".format(name, value))


def assert_upscaling_method(name: str, value: str) -> str:
    if value in ["Gscaler", "DegreeDistribution"]:
        return value
    else:
        raise AssertionError(
            "Invalid upscaling method for {}: `{}`".format(name, value))


def assert_host(name: str, value: str) -> str:
    """
    Makes sure the value is an available host, otherwise raises AssertionError

    :param name: Argument name
    :param value: Value
    :return: Value as string
    """
    # if os.system("ping -c 1 -w2 {} > /dev/null 2>&1".format(value)) != 0:
    if os.system("ping -c 1 {} > /dev/null 2>&1".format(value)) != 0:
        raise AssertionError("Invalid host for {}: `{}`".format(name, value))

    return value


def assert_standard_scale(name: str, value: str) -> float:
    try:
        scale = float(value)
    except ValueError:
        raise AssertionError(f"Invalid value for {name}: `{value}`")

    if scale <= 0.0 or scale >= 1.0:
        raise AssertionError(
            f"The value should be between 0 and 1 for {name}: `{value}`")

    return scale


def assert_master_type(name: str, value: str):
    """
    Makes sure the value represents a Master class otherwise raises AssertionError

    :param name: Argument name
    :param value: Value
    :return: Value as integer
    """
    # nested import to "avoid" circular dependency
    from lab.master.Master import Master
    from lab.master.Upscaler import Upscaler

    MASTER_MAP = {"Master": Master,
                  "Upscaler": Upscaler}

    if value not in MASTER_MAP.keys():
        types = ' | '.join(MASTER_MAP.keys())
        raise AssertionError(f"Expected any of {types} but got {value}")

    return MASTER_MAP[value]


def no_assertion(name: str, value: str) -> str:
    """
    Make no assertion on the value

    :param name: Argument name
    :param value: Value
    :return: Value
    """
    return value
