import json

MAX_MESSAGE_SIZE = 1024
GRAPH = 100
BACKUP = 101

# Status Codes
ALIVE = 200
REGISTER = 201
META_DATA = 202
DEBUG = 203
RANDOM_WALKER = 204
FINISH_JOB = 206  # Master to Worker
JOB_COMPLETE = 207  # Response, Worker to Master
TERMINATE = 208  # Shut down
WORKER_FAILED = 209
RANDOM_WALKER_COUNT = 210
CONTINUE = 211
IGNORE = 212
START_SEND_FILE = 213
FILE_CHUNK = 214
MISSING_CHUNK = 215
RECEIVED_FILE = 216
END_SEND_FILE = 217


def write(status: int, body: dict or list = None):
    if body is None:
        # no content
        body = ''
    return json.dumps({'status': status, 'body': body}).encode()


def write_alive(worker_id: int):
    return write(
        status=ALIVE,
        body={
            'worker_id': worker_id
        }
    )


def write_register(worker_id: int, host: str, port: int):
    return write(
        status=REGISTER,
        body={
            'worker_id': worker_id,
            'host': host,
            'port': port
        }
    )


def write_meta_data(meta_data: list):
    return write(status=META_DATA, body=meta_data)


def write_debug(worker_id: int, debug_message: str):
    return write(
        status=DEBUG,
        body={
            'worker_id': worker_id,
            'debug_message': debug_message
        }
    )


def write_random_walker(vertex_label: int):
    return write(
        status=RANDOM_WALKER,
        body={
            'vertex_label': vertex_label
        }
    )


def write_job(status=JOB_COMPLETE, worker_id=None):
    return write(
        status=status,
        body={
            'worker_id': worker_id
        }
    )


def write_worker_failed():
    return write(
        status=WORKER_FAILED
    )


def write_random_walker_count(worker_id: int, count: int):
    return write(
        status=RANDOM_WALKER_COUNT,
        body={
            'worker_id': worker_id,
            'count': count
        }
    )


def write_continue():
    return write(
        status=CONTINUE
    )


def write_start_send_file(worker_id: int, file_type: int, number_of_chunks: int):
    return write(status=START_SEND_FILE, body={
        'worker_id': worker_id,
        'file_type': file_type,
        'number_of_chunks': number_of_chunks
    })


def write_file_chunk(worker_id: int, file_type: int, index: int, chunk: str):
    return write(status=FILE_CHUNK, body={
        'worker_id': worker_id,
        'file_type': file_type,
        'index': index,
        'chunk': chunk
    })


def write_missing_chunk(worker_id: int, file_type: int, index: int):
    return write(status=MISSING_CHUNK, body={
        'worker_id': worker_id,
        'file_type': file_type,
        'index': index
    })


def write_received_file(worker_id: int, file_type: int):
    return write(
        status=RECEIVED_FILE, body={
            'worker_id': worker_id,
            'file_type': file_type
        }
    )


def write_end_send_file(worker_id: int, file_type: int):
    return write(
        status=END_SEND_FILE,
        body={
            'worker_id': worker_id,
            'file_type': file_type
        }
    )


def read_status(status):
    # Returns a constant function
    return lambda body: (status,)


def read_register(body: dict):
    return REGISTER, body['worker_id'], body['host'], body['port']


def read_alive(body: dict):
    return ALIVE, body['worker_id']


def read_meta_data(body: list):
    return META_DATA, body


def read_debug(body: dict):
    return DEBUG, body['worker_id'], body['debug_message']


def read_random_walker(body: dict):
    return RANDOM_WALKER, body['vertex_label']


def read_job_complete(body: dict):
    return JOB_COMPLETE, body['worker_id']


def read_random_walker_count(body: dict):
    return RANDOM_WALKER_COUNT, body['worker_id'], body['count']


def read_start_send_file(body: dict):
    return START_SEND_FILE, body['worker_id'], body['file_type'], body['number_of_chunks']


def read_file_chunk(body: dict):
    return FILE_CHUNK, body['worker_id'], body['file_type'], body['index'], body['chunk']


def read_missing_chunk(body: dict):
    return MISSING_CHUNK, body['worker_id'], body['file_type'], body['index']


def read_received_file(body: dict):
    return RECEIVED_FILE, body['worker_id'], body['file_type']


def read_end_send_file(body: dict):
    return END_SEND_FILE, body['worker_id'], body['file_type']


def read(message: bytes):
    content = json.loads(message.decode())

    return MESSAGE_INTERFACE[content['status']](content['body'])


MESSAGE_INTERFACE = {
    ALIVE: read_alive,
    REGISTER: read_register,
    META_DATA: read_meta_data,
    DEBUG: read_debug,
    RANDOM_WALKER: read_random_walker,
    FINISH_JOB: read_status(FINISH_JOB),
    JOB_COMPLETE: read_job_complete,
    TERMINATE: read_status(TERMINATE),
    WORKER_FAILED: read_status(WORKER_FAILED),
    RANDOM_WALKER_COUNT: read_random_walker_count,
    CONTINUE: read_status(CONTINUE),
    START_SEND_FILE: read_start_send_file,
    RECEIVED_FILE: read_received_file,
    FILE_CHUNK: read_file_chunk,
    MISSING_CHUNK: read_missing_chunk,
    END_SEND_FILE: read_end_send_file
}
