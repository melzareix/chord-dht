from enum import Enum
from loguru import logger
import struct


class DhtMessageCodes(Enum):
    DHT_PUT = 650
    DHT_GET = 651
    DHT_SUCC = 652
    DHT_FAIL = 653


class ApiService:
    def process_message(self, data):
        sz = struct.unpack(">H", data[:2])[0]

        # check size matche
        if sz != len(data):
            logger.error(
                f"Message Size mismatch. Payload Size {len(data)}, Expected Size {sz}"
            )
            return False

        msg_type = struct.unpack(">H", data[2:4])[0]

        if msg_type == DhtMessageCodes.DHT_PUT.value:
            return self._process_put(data[4:])

        if msg_type == DhtMessageCodes.DHT_GET.value:
            return self._process_get(data[4:])

        logger.error(f"Invalid message Type. Got {msg_type}.")
        return False

    def _process_get(self, data):
        key = data.hex()
        return True

    def _process_put(self, data):
        ttl, replication, _, key = struct.unpack(">HBBQ", data[:12])
        value = data[12:].decode()  # TODO:: error handling
        print(ttl, replication, key, value)
        return self._create_succ(key, value)

    def _create_fail(self, key):
        return struct.pack(">HHQ", 12, DhtMessageCodes.DHT_FAIL.value, key)

    def _create_succ(self, key, value):
        value_bytes = bytes(value, "utf-8")
        length = len(value_bytes) + 12
        return (
            struct.pack(">HHQ", length, DhtMessageCodes.DHT_SUCC.value, key)
            + value_bytes
        )
