import sys


def get_arg(name: str, assertion):
    try:
        index = sys.argv.index(name)
        value = sys.argv[index + 1]
    except ValueError:
        raise NameError("Unable to find argument: {}".format(name))
    except IndexError:
        raise IndexError("No value found for argument: {}".format(name))

    value = assertion(name, value)
    return value
