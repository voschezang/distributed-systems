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
    return int(out.strip(b' ').partition(b' ')[0])


def read_n_lines(f: TextIO, n: int) -> [str]:
    """
    Reads n lines into a list

    :param f: File to read from
    :param n: Number of lines to read
    :return: List of n lines
    """

    return [next(f) for _ in range(n)]


def get_first_line(path: str):
    return subprocess.check_output(['head', '-1', path]).decode()


def get_last_line(path: str):
    return subprocess.check_output(['tail', '-1', path]).decode()


def get_start_vertex(edge: str):
    """
    Retrieves the start vertex of an edge

    :param edge: Edge to get the start vertex from. Has the form "start_vertex end_vertex"
    :return: Start vertex
    """

    try:
        return edge.split(" ")[0]
    except IndexError:
        return None


def read_rest_of_edges(f: TextIO, start_vertex: str):
    """
    Reads in the rest of the edges that have the same start vertex
    :param f: File to read from
    :param start_vertex: Start vertex to look out for
    :return: Next start edge, rest of edges with the same start vertex as `start_vertex`
    """

    rest_of_edges = []
    while True:
        current_edge = next(f)

        # EOF
        if not current_edge:
            return None, rest_of_edges

        # Return if new start vertex has been found
        if get_start_vertex(current_edge) != start_vertex:
            return current_edge, rest_of_edges

        rest_of_edges.append(current_edge)


def read_in_chunks(f: TextIO, n_workers: int) -> [str]:
    """
    Generator that divides a file into n_workers parts, where each part contains all the edges from each start vertex

    :param f: File to read from
    :param n_workers: Number of parts to divide the files in
    :return: List of lines
    """
    number_of_lines = get_number_of_lines(f.name)

    chunk_size = int(floor(number_of_lines / n_workers))

    lines_read = 0
    edges = []
    for i in range(n_workers - 1):
        # Get chunk
        edges += read_n_lines(f, chunk_size)

        # Get rest of edges with the same start vertex and the new edge for the next worker
        new_edge, rest_of_edges = read_rest_of_edges(f, get_start_vertex(edges[-1]))

        edges += rest_of_edges
        lines_read += len(edges)

        # Return part for worker
        yield edges

        # New collection starts with the last found edge
        edges = [new_edge]

    # Return the last found edge and the rest of the edges in the file (-1 is for the last found edge)
    yield edges + read_n_lines(f, number_of_lines - lines_read - 1)


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
