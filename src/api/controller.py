import asyncio

from loguru import logger

from .service import ApiService


class ApiController(asyncio.Protocol):
    def __init__(self, chord_node):
        self.transport = None
        self.service = ApiService(chord_node)

    def connection_made(self, transport):
        self.transport = transport

    async def process_data(self, data):
        result = await self.service.process_message(data)
        logger.debug(f"API Result: {result}")
        if not result:
            return self.close_connection()
        if isinstance(result, str):
            result = result.encode("utf-8")

        self.transport.write(result)

    def data_received(self, data):
        asyncio.ensure_future(self.process_data(data))

    def close_connection(self):
        if self.transport:
            self.transport.close()
            self.transport = None
