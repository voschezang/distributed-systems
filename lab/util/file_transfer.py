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

        self.file += chunk.split('\n')
        self.expected_chunk_index += 1

    def handle_end_send_file(self):
        if not self.received_complete_file:
            raise UnexpectedChunkIndex('Missing chunk(s) at end send file', self.expected_chunk_index)
