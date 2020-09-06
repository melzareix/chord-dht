import asyncio
import sys

import aiomas
import nest_asyncio
from loguru import logger

from api.controller import ApiController
from chord.node import Node
from config.config import dht_config


async def _start_api_server(host: str, port: str, chord_node: Node):
    loop = asyncio.get_running_loop()
    server = await loop.create_server(lambda: ApiController(chord_node), host, int(port))
    logger.info(f"API Server started: {host}:{port}")
    # await server.serve_forever()
    return server


async def _start_chord():
    chord_node = Node(host="localhost", port=sys.argv[1])
    if len(sys.argv) >= 3:
        await chord_node.join(bootstrap_node=f"localhost:{sys.argv[2]}")
    else:
        await chord_node.join(bootstrap_node=None)
    chord_node.dump_me()

    chord_rpc_server = aiomas.run(
        until=aiomas.rpc.start_server(
            ("localhost", int(sys.argv[1])),
            chord_node,
        )
    )
    logger.info(f"Chord RPC Server start at: localhost:{sys.argv[1]}")
    return chord_rpc_server, chord_node


async def _start():
    api_address = dht_config["api_address"]

    api_host = api_address.split(":")[0]
    api_port = int(api_address.split(":")[1]) + int(sys.argv[3] if len(sys.argv) > 2 else 0)

    nest_asyncio.apply()
    rpc_server, chord_node = await _start_chord()
    api_server = await _start_api_server(api_host, str(api_port), chord_node)

    loop = asyncio.get_event_loop()
    stabilize_task = loop.create_task(chord_node.stabilize())
    fix_fingers_task = loop.create_task(chord_node.fix_fingers())

    async with api_server, rpc_server:
        await asyncio.gather(api_server.serve_forever(), rpc_server.wait_closed(),
                             loop.run_until_complete(stabilize_task), loop.run_until_complete(fix_fingers_task))


async def _test_api_only():
    api_address = dht_config["api_address"]
    api_host = api_address.split(":")[0]
    api_port = int(api_address.split(":")[1]) + int(sys.argv[3] if len(sys.argv) > 2 else 0)
    api_server = await _start_api_server(api_host, api_port, None)


if __name__ == "__main__":
    asyncio.run(_start())
    # asyncio.run(_test_api_only())
