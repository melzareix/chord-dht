import asyncio

from loguru import logger

from .service import ApiService


class ApiController(asyncio.Protocol):
    """
    This class represent the API controller (The entry class).
    """

    def __init__(self, chord_node):
        self.transport = None
        self.service = ApiService(chord_node)

    def connection_made(self, transport):
        """
        sets the connection.
        """
        self.transport = transport

    async def process_data(self, data):
        """
        Unpacks the bytes message and extracts/parses the different fields.
            Args:
                data (bytes): The message in bytes.
            Returns:
                result (string): The result from processing the arg data
                (if one exists).
        """
        result = await self.service.process_message(data)
        logger.debug(f"API Result: {result}")

        if not result:
            return self.close_connection()

        if isinstance(result, str):
            result = result.encode("utf-8")

        if self.transport:
            self.transport.write(result)

        return result

    def data_received(self, data):
        asyncio.ensure_future(self.process_data(data))

    def close_connection(self):
        """
        Closes the currently open connection.
        """
        if self.transport:
            self.transport.close()
            self.transport = None
