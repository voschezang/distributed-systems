import sys

from lab.util.validation import no_assertion


def get_arg(name: str, assertion=no_assertion):
    """
    Parses an argument by first checking if the argument name is mentioned. Than making sure a value
    exist for it. And finally making sure the value is as expected using the assertion.
    :param name: Name of argument
    :param assertion: Assertion
    :return: Value as expected type
    """

    try:
        index = sys.argv.index(name)
        value = sys.argv[index + 1]
    except ValueError:
        raise NameError("Unable to find argument: {}".format(name))
    except IndexError:
        raise IndexError("No value found for argument: {}".format(name))

    value = assertion(name, value)
    return value
