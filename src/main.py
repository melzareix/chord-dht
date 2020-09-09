import argparse
import asyncio
import os

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


async def _start_chord_node(args):
    """
    Start Chord Node
    """
    if args.dht_address:
        dht_address = args.dht_address
    else:
        dht_address = dht_config["listen_address"]

    host, port = dht_address.split(":")
    return host, int(port), Node(host=host, port=port)


async def _start(args: argparse.Namespace):
    nest_asyncio.apply()

    dht_host, dht_port, chord_node = await _start_chord_node(args)
    loop = asyncio.get_event_loop()

    stabilize_task = loop.create_task(chord_node.stabilize())
    fix_fingers_task = loop.create_task(chord_node.fix_fingers())
    check_pred_task = loop.create_task(chord_node.check_predecessor())

    if args.bootstrap_node:
        await chord_node.join(bootstrap_node=args.bootstrap_node)
    else:
        await chord_node.join(bootstrap_node=None)

    # SSL
    tls_dir = os.environ.get("TLS_DIR", "node_1")
    certs_dir = os.path.join(os.path.dirname(__file__), f"./tls/{tls_dir}")
    print(certs_dir)
    server_ctx = aiomas.util.make_ssl_server_context(
        cafile=os.path.join(certs_dir, "ca.pem"),
        certfile=os.path.join(certs_dir, "node.pem"),
        keyfile=os.path.join(certs_dir, "node.key"),
    )

    chord_rpc_server = await aiomas.rpc.start_server((dht_host, dht_port), chord_node, ssl=server_ctx)

    logger.info(f"Chord RPC Server start at: {dht_host}:{dht_port}")

    if args.start_api:
        if not args.api_address:
            api_address = dht_config["api_address"]
        else:
            api_address = args.api_address
        api_host = api_address.split(":")[0]
        api_port = int(api_address.split(":")[1])
        api_server = await _start_api_server(api_host, str(api_port), chord_node)
        async with api_server, chord_rpc_server:
            await asyncio.gather(
                api_server.serve_forever(),
                loop.run_until_complete(chord_rpc_server.serve_forever()),
                loop.run_until_complete(stabilize_task),
                loop.run_until_complete(fix_fingers_task),
                loop.run_until_complete(check_pred_task),
            )
    else:
        async with chord_rpc_server:
            await asyncio.gather(
                loop.run_until_complete(chord_rpc_server.serve_forever()),
                loop.run_until_complete(stabilize_task),
                loop.run_until_complete(fix_fingers_task),
                loop.run_until_complete(check_pred_task),
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start-api", help="If not present won't start an API server.", action="store_true", default=False,
    )
    parser.add_argument("--dht-address", help="Address to run the DHT Node on")
    parser.add_argument("--api-address", help="Address to run the DHT Node on", default=None)
    parser.add_argument(
        "--bootstrap-node", help="Start a new Chord Ring if argument no present", default=None,
    )
    arguments = parser.parse_args()
    asyncio.run(_start(arguments))
