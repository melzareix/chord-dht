import asyncio

import pytest

from chord.helpers import generate_id
from chord.storage import Storage


def convert_key_val(key: str, val: str):
    hex_key = generate_id(key)
    hex_val = val.encode("utf-8").hex()
    return hex_key, hex_val


@pytest.fixture(scope="session", autouse=True)
def storage():
    node_id = "test_node"
    storage = Storage(node_id=node_id)
    return storage


@pytest.mark.asyncio
async def test_puts_key(storage):
    key, val = convert_key_val("hello", "world")
    is_put = storage.put_key(key, val)
    assert is_put

    # check that key is in storage
    found = False
    for k in storage._store.iterkeys():
        if k == key:
            found = True
            break

    assert found

    assert storage._store.get(key) == val


@pytest.mark.asyncio
async def test_expires_after_ttl(storage):
    key, val = convert_key_val("hello2", "world2")
    is_put = storage.put_key(key, val, ttl=1)
    assert is_put

    await asyncio.sleep(1)

    # check that key returns None after ttl expires
    assert storage._store.get(key, default=None) is None


@pytest.mark.asyncio
async def test_gets_key(storage):
    key, val = convert_key_val("hello2", "world2")
    storage.put_key(key, val)
    assert storage.get_key(key) == val
