import subprocess
from typing import TextIO
from math import floor


def get_number_of_lines(path: str) -> int:
    """
    Uses the bash wc command to count the number of lines in the file

    :param path: Path to the file
    :return: Number of lines in the file
    """

    out = subprocess.Popen(['wc', '-l', path],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT
                           ).communicate()[0]

    return int(out.partition(b' ')[0])


def read_n_lines(f: TextIO, n: int) -> [str]:
    """
    Reads n lines into a list

    :param f: File to read from
    :param n: Number of lines to read
    :return: List of n lines
    """
    return [next(f) for _ in range(n)]


def read_in_chunks(f: TextIO, n_workers: int) -> [str]:
    """
    Generator that divides a file into n_workers parts

    :param f: File to read from
    :param n_workers: Number of parts to divide the files in
    :return: List of lines
    """
    number_of_lines = get_number_of_lines(f.name)

    chunk_size = int(floor(number_of_lines / n_workers))

    for i in range(n_workers - 1):
        yield read_n_lines(f, chunk_size)

    yield read_n_lines(f, number_of_lines - (chunk_size * n_workers - 1))


def write_chunk(worker_id: int, data: [str]) -> str:
    """
    Writes a list of lines to a tmp file

    :param worker_id: Unique id for the file
    :param data: Data to write to the file
    :return: Path to the created file
    """

    path = "/tmp/sub_graph_{}.txt".format(worker_id)

    f = open(path, "w")
    f.writelines(data)
    f.close()

    return path
