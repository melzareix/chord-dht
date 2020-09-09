import hashlib
import hmac
import os
from binascii import unhexlify

from diskcache import Cache
from loguru import logger


class Storage:

    def make_digest(self, message: bytes) -> str:
        secret_key = os.environ.get("SEC_KEY", self.node_id)
        return hmac.new(
            secret_key.encode("utf-8"),
            message,
            hashlib.sha256,
        ).hexdigest()

    def __init__(self, node_id: str):
        self._store = Cache('./chord_data')
        self.node_id = node_id

    def get_key(self, key: str):
        value = None
        try:
            value, tag = self._store.get(f"{key}", tag=True)
            if value:
                _val_bytes = unhexlify(value)
                logger.debug(f"Got {value} with digest {tag} - {self.make_digest(_val_bytes)}")
                if tag != self.make_digest(_val_bytes):
                    return None
        except (TimeoutError, AttributeError) as e:
            logger.error(e)
            pass
        return value

    def put_key(self, key: str, value: str, ttl: int = 3600) -> bool:
        _byte_val = unhexlify(value)
        logger.debug(f"Saving Key: {key} with ttl {ttl}secs")
        try:
            return self._store.set(key, value=value, expire=ttl, tag=self.make_digest(_byte_val))
        except Exception as e:
            logger.error(e)
            return False
