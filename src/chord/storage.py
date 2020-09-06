import os

from simplekv.crypt import HMACDecorator
from simplekv.decorator import PrefixDecorator
from simplekv.fs import FilesystemStore


class Storage:
    def __init__(self, node_id: str):
        secret_key = os.environ.get("HMAC_KEY", "secret_key").encode()
        fs_store = FilesystemStore('./chord_data')
        prefix_store = PrefixDecorator(prefix=node_id, store=fs_store)
        self._store = HMACDecorator(secret_key=secret_key, decorated_store=prefix_store)
        del secret_key

    def get_key(self, key: str):
        value = None
        try:
            value = self._store.get(key).decode()
        except KeyError:
            pass
        return value

    def put_key(self, key: str, value: str):
        return self._store.put(key, value.encode())
