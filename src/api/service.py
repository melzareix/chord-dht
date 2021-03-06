import struct
from binascii import unhexlify
from enum import Enum

from loguru import logger

from chord.node import Node


class DhtMessageCodes(Enum):
    DHT_PUT = 650
    DHT_GET = 651
    DHT_SUCC = 652
    DHT_FAIL = 653


class ApiService:
    """
    This class represents the API service.
    """

    def __init__(self, chord_node: Node):
        self.chord_node = chord_node

    async def process_message(self, data):
        """
        Unpacks the bytes message and extracts/parses the different fields.
            Args:
                data (bytes): The message in bytes.
        """
        sz = struct.unpack(">H", data[:2])[0]

        # check size match
        if sz != len(data):
            logger.error(f"Message Size mismatch. Payload Size {len(data)}, Expected Size {sz}")
            return False

        msg_type = struct.unpack(">H", data[2:4])[0]
        logger.info(f"Got new message: {msg_type} => {DhtMessageCodes(msg_type)}")
        if msg_type == DhtMessageCodes.DHT_PUT.value:
            await self._process_put(data[4:])
            return

        if msg_type == DhtMessageCodes.DHT_GET.value:
            return await self._process_get(data[4:])

        logger.error(f"Invalid message Type. Got {msg_type}.")
        return False

    async def _process_get(self, data: bytes):
        """
        Unpacks/parses and precesses the bytes message (the key) and
        attempts to get the value stored under that key from chord.
        (if one exists)
            Args:
                data (bytes): The message in bytes (which is actually the key).
            Returns:
                data (bytes): A success message containing the status code
                DHT_SUCC, the key and the value.
        """
        key = data
        val = await self.chord_node.find_key(key.hex())
        if not val:
            return self._create_fail(key)
        return self._create_succ(key, unhexlify(val))

    async def _process_put(self, data: bytes):
        """
        Unpacks/parses and precesses the bytes message (the key) and
        creates a put request to the chord network to store the value under the
        given key.
            Args:
                data (bytes): The message in bytes (which is actually the key).
            Returns:
                keys (list): The updated list of keys
        """
        ttl, replication, _ = struct.unpack(">HBB", data[:4])

        key = data[4:36].hex()
        value = data[36:].hex()

        logger.info(
            f"Handling put message: Hex Key {key} [TTL {ttl}, replication {replication}] => Hex Value [{value}]"
        )
        await self.chord_node.put_key(key, value, int(ttl))

    @staticmethod
    def _create_fail(key):
        """
        Generates a create failed message and packs it into bytes.
        given key.
            Args:
                key (bytes): The key as bytes
            Returns:
                create_failed (bytes): a byte message with DHT_FAIL status code
                and key
        """
        print("kk", len(key))
        length = len(key) + 4
        return struct.pack(">HH", length, DhtMessageCodes.DHT_FAIL.value) + key

    @staticmethod
    def _create_succ(key, value):
        """
        Generates a create success message and packs it into bytes.
        given key.
            Args:
                key (bytes): The key as bytes
                value (string): The value stored under the given key.
            Returns:
                create_success (bytes): a byte message with DHT_SUCC status
                code, the key and the value.
        """
        length = len(value) + len(key) + 4
        return struct.pack(">HH", length, DhtMessageCodes.DHT_SUCC.value) + key + value
