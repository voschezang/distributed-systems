import asyncio
import websockets


class Communication:
    def __init__(self, data):
        self.data = data
        self.master_server_domain = self.data['master_server_domain']
        self.master_server_port = self.data['master_server_port']

    def receive_graph(self):
        return self.receive()

    def receive(self, domain='localhost', port=8890):
        async def receive(websocket, path):
            message = await websocket.recv()
            print(message)
        start_server = websockets.serve(receive, domain, port)

        asyncio.get_event_loop().run_until_complete(start_server)

    def send(self, message):
        async def hello(message):
            uri = "ws://{}:{}".format(self.master_server_domain,
                                      self.master_server_port)
            async with websockets.connect(uri) as websocket:
                await websocket.send(message)

        asyncio.get_event_loop().run_until_complete(hello(message))

    def encode(self, data):
        return data

    def decode(self, data):
        return data
