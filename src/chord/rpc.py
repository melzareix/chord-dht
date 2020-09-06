from typing import Optional, List

import aiomas
from loguru import logger

from chord.helpers import gen_finger


######################
# RPC Procedures
######################

async def rpc_ask_for_succ(next_node: dict, numeric_id: int) -> (bool, Optional[dict]):
    try:
        host, port = next_node["addr"].split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        found, rep = await rpc_con.remote.find_successor(numeric_id)
        print(found, rep)
        await rpc_con.close()
        return found, rep
    except Exception as e:
        logger.error(e)
        return False, None


# async def rpc_ask_for_pred(addr: str) -> Optional[dict]:
#     try:
#         host, port = addr.split(":")
#         rpc_con = await aiomas.rpc.open_connection((host, port))
#         rep = await rpc_con.remote.get_predecessor()
#         await rpc_con.close()
#         return rep
#     except Exception as e:
#         logger.error(e)
#         return None
#

async def rpc_ask_for_pred_and_succlist(addr: str) -> (Optional[dict], List):
    try:
        host, port = addr.split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.get_pred_and_succlist()
        await rpc_con.close()
        return rep
    except Exception as e:
        logger.error(e)
        return None, []


async def rpc_ping(addr: str) -> bool:
    try:
        host, port = addr.split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.ping()
        await rpc_con.close()
        return rep == "pong"
    except Exception as e:
        logger.error(e)
        return False


async def rpc_notify(succ_addr: str, my_addr: str) -> None:
    try:
        host, port = succ_addr.split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        await rpc_con.remote.notify(gen_finger(my_addr))
        await rpc_con.close()
    except Exception as e:
        pass


async def rpc_get_key(next_node: dict, key: str, ttl: int) -> Optional[str]:
    try:
        host, port = next_node["addr"].split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.find_key(key, ttl)
        logger.info("response from node =>", rep)
        await rpc_con.close()
        return rep
    except Exception as e:
        logger.error(e)
        return None


async def rpc_save_key(next_node: dict, key: str, value: str) -> Optional[str]:
    try:
        host, port = next_node["addr"].split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.save_key(key, value)
        await rpc_con.close()
        return rep
    except Exception as e:
        logger.error(e)
        return None


async def rpc_put_key(next_node: dict, key: str, value: str) -> Optional[str]:
    try:
        host, port = next_node["addr"].split(":")
        rpc_con = await aiomas.rpc.open_connection((host, port))
        rep = await rpc_con.remote.put_key(key, value)
        await rpc_con.close()
        return rep
    except Exception as e:
        logger.error(e)
        return None
