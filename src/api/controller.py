import asyncio
import struct
from .service import ApiService


class ApiController(asyncio.Protocol):
    def __init__(self):
        self.transport = None
        self.service = ApiService()

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        result = self.service.process_message(data)

        if not result:
            return self.close_connection()

        self.transport.write(result)

    def close_connection(self):
        if self.transport:
            self.transport.close()
            self.transport = None
