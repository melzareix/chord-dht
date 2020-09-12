import hashlib
import hmac
import os
from binascii import unhexlify
from typing import List

from diskcache import Cache
from loguru import logger

from chord.helpers import between, generate_id


class Storage:
    def make_digest(self, message: bytes) -> str:
        secret_key = os.environ.get("SEC_KEY", self.node_id)
        return hmac.new(secret_key.encode("utf-8"), message, hashlib.sha256,).hexdigest()

    def __init__(self, node_id: str):
        self._store = Cache("./chord_data")
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

    def _del_key(self, key):
        return self._store.delete(key)

    def del_keys(self, keys: List[str]):
        for key in keys:
            self._del_key(key)

    def get_my_data(self):
        keys = []
        values = []
        for key in self._store.iterkeys():
            val = self.get_key(key)
            if val:
                keys.append(key)
                values.append(val)
        return keys, values

    def get_keys(self, left: int, right: int):
        keys = []
        values = []
        logger.debug(list(self._store.iterkeys()))
        for key in self._store.iterkeys():
            logger.debug(
                f"{key} - ({left}, {right}) => {between(int(key, 16), left, right, inclusive_left=False, inclusive_right=False)}"
            )
            if between(int(key, 16), left, right, inclusive_left=False, inclusive_right=False):
                val = self.get_key(key)
                if val:
                    keys.append(key)
                    values.append(val)

        logger.debug(f"Got {keys} => {values}")
        return keys, values

    def put_keys(self, keys, values):
        for idx, key in enumerate(keys):
            self.put_key(key, values[idx])

    def gen_keys(self, key: str, replicas: int) -> List[str]:
        new_key = key
        keys = []
        for i in range(replicas):
            new_key = generate_id(key)
            keys.append(new_key)
        return keys
