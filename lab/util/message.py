import json


# Status Codes
ALIVE = 200
REGISTER = 201
META_DATA = 202
DEBUG = 203
JOB_COMPLETE = 204


def write(status: int, body: dict or list):
    return json.dumps({'status': status, 'body': body}).encode()


def no_content(status: int, worker_id: int):
    return write(
        status=status,
        body={'worker_id': worker_id}
    )


def write_alive(worker_id: int):
    return no_content(ALIVE, worker_id)


def write_job_complete(worker_id: int):
    return no_content(JOB_COMPLETE, worker_id)


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


def read(message: bytes):
    content = json.loads(message.decode())

    return MESSAGE_INTERFACE[content['status']](content['body'])


def read_register(body: dict):
    return REGISTER, body['worker_id'], body['host'], body['port']


def read_alive(body: dict):
    return ALIVE, body['worker_id']


def read_meta_data(body: list):
    return META_DATA, body


def read_debug(body: dict):
    return DEBUG, body['worker_id'], body['debug_message']


def read_job_complete(body: dict):
    return JOB_COMPLETE, body['worker_id']


MESSAGE_INTERFACE = {
    ALIVE: read_alive,
    REGISTER: read_register,
    META_DATA: read_meta_data,
    DEBUG: read_debug,
    JOB_COMPLETE: read_job_complete
}
