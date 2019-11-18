import socket


def get_hostname():
    return socket.gethostname()


def get_port(s):
    s.listen(1)
    port = s.getsockname()[1]

    return port


def bind(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))

    return s


def connect(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))

    return s


def get_message(s):
    client_socket, addr = s.accept()
    message = client_socket.recv(1024)
    client_socket.close()

    return message.decode()