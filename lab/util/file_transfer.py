from lab.util import file_io, message


class UnexpectedChunkIndex(Exception):
    def __init__(self, message, expected_index):

        # Call the base class constructor with the parameters it needs
        super(UnexpectedChunkIndex, self).__init__(message)

        self.expected_index = expected_index


class FileReceiver:
    def __init__(self, expected_number_of_chunks: int):
        self.file = []
        self.expected_number_of_chunks = expected_number_of_chunks
        self.expected_chunk_index = 0

    @property
    def received_complete_file(self):
        return self.expected_chunk_index >= self.expected_number_of_chunks

    def receive_chunk(self, index: int, chunk: str):
        if index != self.expected_chunk_index:
            raise UnexpectedChunkIndex(f'Unexpected chunk index: {index}, expected {self.expected_chunk_index}', self.expected_chunk_index)

        self.file += [line + '\n' for line in chunk.rstrip().split('\n')]
        self.expected_chunk_index += 1

    def handle_end_send_file(self):
        if not self.received_complete_file:
            raise UnexpectedChunkIndex('Missing chunk(s) at end send file', self.expected_chunk_index)


class FileSender:
    def __init__(self, worker_id: int, file_type: int, data: list):
        self.messages = self.create_messages(worker_id, data, file_type)
        self.target_received_file = False
        self.index = 0

    @property
    def complete_file_send(self):
        return self.index >= len(self.messages)

    @staticmethod
    def get_file_chunk(worker_id, file_type, index, data: list):
        chunk = ''
        lines = 0

        for line in data:
            if len(message.write_file_chunk(worker_id, file_type, index, chunk + line)) >= message.MAX_MESSAGE_SIZE:
                break

            chunk += line
            lines += 1

        return chunk, lines

    def create_messages(self, worker_id: int, data: list, file_type: int):
        messages = []
        while len(data) > 0:
            chunk, lines_in_chunk = self.get_file_chunk(worker_id, file_type, len(messages), data)
            del data[:lines_in_chunk]
            messages.append(message.write_file_chunk(worker_id, file_type, len(messages), chunk))

        return messages

    def get_next_message(self):
        next_message = self.messages[self.index]
        self.index += 1

        return next_message
