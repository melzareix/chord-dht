import asyncio

from chord.helpers import generate_id, between, print_table
from chord.rpc import *
from chord.storage import Storage
from config.config import dht_config


class Node:
    router = aiomas.rpc.Service()

    def __init__(self, host: str, port: str):
        self._addr = f"{host}:{port}"
        self._id = generate_id(self._addr.encode("utf-8"))
        self._numeric_id = int(self._id, 16)

        self._MAX_STEPS = int(dht_config['max_steps'])
        self._MAX_SUCC = int(dht_config['max_succ'])

        self._fingers = [{"addr": "", "id": "", "numeric_id": -1} for _ in range(int(dht_config['finger_table_sz']))]

        self._predecessor = None
        self._successor = None

        self._storage = Storage(node_id=self._id)

        # for stabilization
        self._successors = [None for _ in range(self._MAX_SUCC)]
        self._next = 0

    ##################################
    # Node Initialization(s)
    ##################################

    def _init_empty_fingers(self):
        """
        Generate empty finger table with my address as fingers.
        """
        addr = self._successor["addr"] if self._successor else self._addr
        _id = generate_id(addr.encode("utf-8"))
        for i in range(int(dht_config['finger_table_sz'])):
            self._fingers[i] = {
                "addr": addr,
                "id": _id,
                "numeric_id": int(_id, 16)
            }

        self._successor = self._fingers[0]
        self._successors = [self._successor.copy() for _ in range(len(self._successors))]

    async def join(self, bootstrap_node: Optional[str]):
        # Create a new ring if no bootstrap node is given
        if not bootstrap_node:
            logger.debug("Bootstrap node initialized...")
            self._create()
        else:
            if self._successor is None:
                _, self._successor = await rpc_ask_for_succ(gen_finger(bootstrap_node), self._numeric_id)
                self._init_empty_fingers()
                print_table(self._fingers)
            else:
                raise Exception("Attempting to join after joining before.")

    def _create(self):
        self._predecessor = None
        self._init_empty_fingers()

    ##################################
    # Find Successor
    ##################################

    def _closest_preceding_node(self, numeric_id: int):
        for i in range(len(self._fingers) - 1, -1, -1):
            if self._fingers[i]["numeric_id"] != -1:
                if between(self._fingers[i]["numeric_id"], self._numeric_id, numeric_id, inclusive_left=False,
                           inclusive_right=True):
                    # logger.info(f"Using finger {i} => {self._fingers[i]}")
                    return self._fingers
        return self._successor

    def _find_successor(self, _numeric_id: int):
        is_bet = between(_numeric_id, self._numeric_id, self._successor["numeric_id"], inclusive_right=True)
        # logger.debug(f"Finding succ for: {_numeric_id} using node {self._numeric_id}: {is_bet}")
        if is_bet:
            return True, self._successor
        return False, self._closest_preceding_node(_numeric_id)

    @aiomas.expose
    async def find_successor(self, numeric_id: int):
        found, next_node = self._find_successor(numeric_id)
        i = 0
        while not found and i < self._MAX_STEPS:
            found, next_node = await rpc_ask_for_succ(next_node, numeric_id)
            i += 1
        if found:
            return True, next_node
        return False, None

    ##################################
    # Network Stabilization
    ##################################

    async def check_predecessor(self):
        res = await rpc_ping()
        if not res:
            self._predecessor = None

    @aiomas.expose
    def get_pred_and_succlist(self):
        return self._predecessor, self._successors

    async def stabilize(self):
        # if succ not yet set don't run stabilize
        _fix_interval = int(dht_config['fix_interval'])
        while True:
            await asyncio.sleep(_fix_interval)
            if not self._successor:
                continue
            # logger.info("Stabilizing the network")
            pred, succ_list = await rpc_ask_for_pred_and_succlist(self._successor["addr"])
            if pred is not None:
                if between(pred["numeric_id"], self._numeric_id, self._successor["numeric_id"], inclusive_right=False,
                           inclusive_left=False):
                    self._successor = pred.copy()
                    self._fingers[0] = self._successor
            self._successors = [self._successor] + succ_list[1:]
            await rpc_notify(self._successor["addr"], self._addr)
            # logger.debug("Dumping after stabilizing the network...")
            # self.dump_me()
            # logger.info(f"Sleeping for {SECS_TO_WAIT} secs before stabilizing again")

    async def fix_fingers(self):
        _fix_interval = int(dht_config['fix_interval'])
        while True:
            await asyncio.sleep(_fix_interval)
            self._next = (self._next + 1) % len(self._fingers)
            next_id = (self._numeric_id + (2 ** self._next)) % (2 ** len(self._fingers))
            found, succ = await self.find_successor(next_id)
            # logger.info(f"Result for fixing finger {self._next} {next_id} => {found} {succ}")
            if not found:
                logger.warning("No suitable node found to fix this finger.")
            else:
                if self._fingers[self._next] != succ:
                    # logger.info(f"Finger {self._next} updated from {self._fingers[self._next]['addr']} to {succ}.")
                    self._fingers[self._next] = succ
                    # TODO: optimization need to check for correctness
                    for i in range(self._next + 1, len(self._fingers)):
                        __id = (self._numeric_id + (2 ** i)) % (2 ** len(self._fingers))
                        if between(__id, self._numeric_id, succ["numeric_id"], inclusive_right=True):
                            self._fingers[i] = succ
                            # print(f"fixed {i} via {self._next}")
            # print_table(self._fingers)

    @aiomas.expose
    def notify(self, n):
        if not self._predecessor or between(n["numeric_id"], self._predecessor["numeric_id"], self._numeric_id,
                                            inclusive_left=False, inclusive_right=False):
            self._predecessor = n

    @aiomas.expose
    def save_key(self, key: str, value: str):
        logger.info(f"Saving key {key} => {value} in my storage.")
        return self._storage.put_key(key, value)

    @aiomas.expose
    async def put_key(self, key: str, value: str):
        dht_key = generate_id(key)
        numeric_id = int(dht_key, 16)
        logger.warning(f"Putting Key: {key} - {dht_key} - {numeric_id}")
        found, next_node = await self.find_successor(numeric_id)
        if not found:
            return None
        logger.info(f"putting key {dht_key} on node {next_node['addr']}")
        return await rpc_save_key(next_node, dht_key, value)

    @aiomas.expose
    async def find_key(self, key: str, ttl: int = 4):
        logger.debug(f"Finding key TTL {ttl} {key}")
        if ttl <= 0:
            return None
        dht_key = generate_id(key)
        numeric_id = int(dht_key, 16)
        logger.warning(f"Getting Key: {key} - {dht_key} - {numeric_id}")
        found, value = self._find_key(dht_key)
        if found:
            return value
        found, node = await self.find_successor(numeric_id)
        if not found:
            return None
        logger.debug(f"Getting key from responsible node {node}")
        return await rpc_get_key(node, key, ttl - 1)

    def _find_key(self, key: str):
        value = self._storage.get_key(key)
        logger.info(f"finding key {key} => {value}")
        if value is not None:
            return True, value
        # get the succ responsible for the key
        return False, None

    @staticmethod
    def ping():
        return "pong"

    def dump_me(self):
        logger.debug("My data & Fingers")
        print_table([{
            "addr": self._addr,
            "id": self._id,
            "numeric_id": self._numeric_id
        }, self._successor, self._predecessor])
