import pytest

from chord.helpers import generate_id
from chord.node import Node


def convert_key_val(key: str, val: str):
    hex_key = generate_id(key)
    hex_val = val.encode("utf-8").hex()
    return hex_key, hex_val


key, val = convert_key_val("test_node", "node_value")
shared_data = {"key": key, "value": val, "keys": []}


@pytest.fixture(autouse=True)
def node(mocker):
    node = Node("localhost", "5000")
    return node


@pytest.mark.asyncio
async def test_node_put_key(node, mocker):
    def mock_rpc_save_key(**kwargs):
        return node.save_key(key=kwargs["key"], value=kwargs["value"], ttl=kwargs["ttl"])

    await node.join(None)
    mocker.patch(
        "chord.node.rpc_ask_for_succ",
        return_value=(True, {"addr": node._addr, "id": node._id, "numeric_id": node._numeric_id}),
    )
    mocker.patch("chord.node.rpc_save_key", side_effect=mock_rpc_save_key)
    key, val = convert_key_val("test_node", "node_value")

    keys = await node.put_key(key=key, value=val, ttl=3600)
    shared_data["keys"] = keys


@pytest.mark.asyncio
async def test_node_get_key(node, mocker):
    async def mock_rpc_get_key(**kwargs):
        return await node.find_key(key=kwargs["key"], ttl=kwargs["ttl"], is_replica=kwargs["is_replica"])

    await node.join(None)
    mocker.patch(
        "chord.node.rpc_ask_for_succ",
        return_value=(True, {"addr": node._addr, "id": node._id, "numeric_id": node._numeric_id}),
    )
    mocker.patch("chord.node.rpc_get_key", side_effect=mock_rpc_get_key)
    for k in shared_data["keys"]:
        ret_val = await node.find_key(k)
        print(ret_val)
        assert ret_val == shared_data["value"]


@pytest.mark.asyncio
async def test_node_replication(node, mocker):
    async def mock_rpc_get_key(**kwargs):
        return await node.find_key(key=kwargs["key"], ttl=kwargs["ttl"], is_replica=kwargs["is_replica"])

    await node.join(None)
    mocker.patch(
        "chord.node.rpc_ask_for_succ",
        return_value=(True, {"addr": node._addr, "id": node._id, "numeric_id": node._numeric_id}),
    )
    mocker.patch("chord.node.rpc_get_key", side_effect=mock_rpc_get_key)
    node._storage.del_keys(shared_data["keys"][1:])

    ret_val = await node.find_key(shared_data["keys"][0])
    assert ret_val == shared_data["value"]
