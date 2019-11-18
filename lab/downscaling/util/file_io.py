import subprocess
from typing import TextIO

from math import floor


def get_number_of_lines(path: str) -> int:
    out = subprocess.Popen(['wc', '-l', path],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT
                           ).communicate()[0]

    return int(out.partition(b' ')[0])


def read_n_lines(f: TextIO, n: int) -> [str]:
    return [next(f) for _ in range(n)]


def read_in_chunks(f: TextIO, n_workers: int) -> [str]:
    number_of_lines = get_number_of_lines(f.name)

    chunk_size = int(floor(number_of_lines / n_workers))

    for i in range(n_workers - 1):
        yield read_n_lines(f, chunk_size)

    yield read_n_lines(f, number_of_lines - (chunk_size * n_workers - 1))


def write_chunk(worker_id: int, data: [str]) -> str:
    path = "/tmp/sub_graph_{}.txt".format(worker_id)

    f = open(path, "w")
    f.writelines(data)
    f.close()

    return path
