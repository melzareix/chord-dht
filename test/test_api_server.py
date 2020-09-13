import struct

import pytest

from api.controller import ApiController


class MockNode:
    async def find_key(self, key: str):
        if key != "found".encode("utf-8").ljust(32, b"\0").hex():
            return None
        return "value".encode("utf-8").hex()


@pytest.fixture(autouse=True)
def controller():
    return ApiController(MockNode())


@pytest.mark.asyncio
async def test_api_get_non_existing_key(controller, mocker):
    key = "random_key_23283".encode("utf-8").ljust(32, b"\0")
    data = struct.pack(">HH", 36, 651) + key
    res = await controller.process_data(data)
    # assert fail message
    assert res == (struct.pack(">HH", 4 + 32, 653) + key)


@pytest.mark.asyncio
async def test_api_get_existing_key(controller, mocker):
    key = "found".encode("utf-8").ljust(32, b"\0")
    data = struct.pack(">HH", 36, 651) + key
    res = await controller.process_data(data)
    val = "value".encode("utf-8")
    assert res == (struct.pack(">HH", 4 + 32 + len(val), 652) + key + val)
