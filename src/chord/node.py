import asyncio
import os

from chord.helpers import generate_id, between, print_table
from chord.rpc import *
from chord.storage import Storage
from config.config import dht_config


class Node:
    """
    Hello World
    """

    router = aiomas.rpc.Service()

    def __init__(self, host: str, port: str):
        self._addr = f"{host}:{port}"
        self._id = generate_id(self._addr.encode("utf-8"))

        ring_sz = 2 ** (int(dht_config["finger_table_sz"]))
        self._numeric_id = int(self._id, 16) % ring_sz

        self._MAX_STEPS = int(dht_config["max_steps"])
        self._MAX_SUCC = int(dht_config["max_succ"])
        self._REPLICATION_COUNT = 3

        self._fingers = [
            {"addr": "", "id": "", "numeric_id": -1} for _ in range(int(dht_config["finger_table_sz"]))
        ]

        self._predecessor = None
        self._successor = None

        self._storage = Storage(node_id=self._id)

        # for stabilization
        self._successors = [None for _ in range(self._MAX_SUCC)]
        self._next = 0

        tls_dir = os.environ.get("TLS_DIR", "node_1")

        # SSL
        certs_dir = os.path.join(os.path.dirname(__file__), f"../tls/{tls_dir}")
        logger.debug(certs_dir)
        self.client_ssl_ctx = aiomas.util.make_ssl_client_context(
            cafile=os.path.join(certs_dir, "ca.pem"),
            certfile=os.path.join(certs_dir, "node.pem"),
            keyfile=os.path.join(certs_dir, "node.key"),
        )
        self.client_ssl_ctx.check_hostname = False

    ##################################
    # Node Initialization(s)
    ##################################

    def _init_empty_fingers(self):
        """
        Generate empty finger table with my address as fingers.
        """
        addr = self._successor["addr"] if self._successor else self._addr
        _id = generate_id(addr.encode("utf-8"))
        for i in range(int(dht_config["finger_table_sz"])):
            self._fingers[i] = {"addr": addr, "id": _id, "numeric_id": int(_id, 16)}

        self._successor = self._fingers[0]
        self._successors = [self._successor.copy() for _ in range(len(self._successors))]

    async def join(self, bootstrap_node: Optional[str]):
        # Create a new ring if no bootstrap node is given
        if not bootstrap_node:
            logger.debug("Bootstrap node initialized...")
            self._create()
        else:
            if self._successor is None:
                _, self._successor = await rpc_ask_for_succ(
                    gen_finger(bootstrap_node), self._numeric_id, ssl_ctx=self.client_ssl_ctx,
                )
                self._init_empty_fingers()
                # get keys from succ
                keys, values = await rpc_get_all_keys(
                    next_node=self._successor, node_id=self._numeric_id, ssl_ctx=self.client_ssl_ctx,
                )
                self._storage.put_keys(keys, values)
            else:
                raise Exception("Attempting to join after joining before.")

        self.dump_me()

    def _create(self):
        self._predecessor = None
        self._init_empty_fingers()

    ##################################
    # Find Successor
    ##################################

    def _closest_preceding_node(self, numeric_id: int):
        """Find the closest preceding node.
          Args:
              numeric_id (int): The numeric id of the node.

          Returns:
              node (dict): the closest preceding node.
        """
        for i in range(len(self._fingers) - 1, -1, -1):
            if self._fingers[i]["numeric_id"] != -1:
                if between(
                    self._fingers[i]["numeric_id"],
                    self._numeric_id,
                    numeric_id,
                    inclusive_left=False,
                    inclusive_right=False,
                ):
                    # logger.info(
                    #     f"Using finger {i} => {self._fingers[i]} {numeric_id} is between ({self._fingers[i]['numeric_id']},{self._numeric_id}]")
                    return self._fingers[i]
        return self._successor

    def _find_successor(self, _numeric_id: int):
        """
        Find the successor for a given node id if it is within the interval of the
        node itself and its successor. If successor is not in range,
        checks the finger table.
          Args:
              numeric_id (int): The numeric id of the node.

          Returns:
              found (bool): Whether or not a successor exists.
              successor (dict): The successor if it exists.
        """
        is_bet = between(
            _numeric_id,
            self._numeric_id,
            self._successor["numeric_id"],
            inclusive_left=False,
            inclusive_right=True,
        )
        # logger.debug(f"Finding succ for: {_numeric_id} using node {self._numeric_id}: {is_bet}")
        if is_bet:
            return True, self._successor
        return False, self._closest_preceding_node(_numeric_id)

    @aiomas.expose
    async def find_successor(self, numeric_id: int):
        """
        Find the successor for a given node id.
          Args:
              numeric_id (int): The numeric id of the node.

          Returns:
              found (bool): Whether or not a successor exist.
              successor (dict): The successor if it exists.
        """
        found, next_node = self._find_successor(numeric_id)
        i = 0
        while not found and i < self._MAX_STEPS:
            found, next_node = await rpc_ask_for_succ(next_node, numeric_id, ssl_ctx=self.client_ssl_ctx)
            i += 1
        if found:
            return True, next_node
        return False, None

    ##################################
    # Network Stabilization
    ##################################

    async def check_predecessor(self):
        _fix_interval = int(dht_config["fix_interval"])
        while True:
            await asyncio.sleep(_fix_interval)
            if self._predecessor:
                res = await rpc_ping(self._predecessor["addr"], ssl_ctx=self.client_ssl_ctx)
                if not res:
                    self._predecessor = None

    @aiomas.expose
    def get_pred_and_succlist(self):
        """Gets the predecessor and successor list of the current node.
          Returns
              dict: predecessor
              array: The list of successors
        """
        return self._predecessor, self._successors

    async def stabilize(self):
        """
        Called periodically. verifies current node’s successor, and tells the
        successor about the current node.
        """
        # if succ not yet set don't run stabilize
        _fix_interval = int(dht_config["fix_interval"])
        print_interval = 60
        time_since_last_print = print_interval
        while True:
            await asyncio.sleep(_fix_interval)
            if not self._successor:
                continue
            # logger.info("Stabilizing the network")
            try:
                pred, succ_list = await rpc_ask_for_pred_and_succlist(
                    self._successor["addr"], ssl_ctx=self.client_ssl_ctx
                )
                if pred is not None:
                    if between(
                        pred["numeric_id"],
                        self._numeric_id,
                        self._successor["numeric_id"],
                        inclusive_right=False,
                        inclusive_left=False,
                    ):
                        self._successor = pred.copy()
                        self._fingers[0] = self._successor
                self._successors = [self._successor] + succ_list[:-1]
                await rpc_notify(self._successor["addr"], self._addr, ssl_ctx=self.client_ssl_ctx)
            except Exception as e:
                logger.error(e)
                logger.error("Succ is no longer working switch to next succ.")
                logger.info(self._successor)
                self._successors = self._successors[1:]
                if len(self._successors) == 0:
                    self._successors.append(gen_finger(self._addr))
                    self._successor = self._successors[0].copy()
                else:
                    self._successor = self._successors[0].copy()

            # logger.debug("Dumping after stabilizing the network...")
            time_since_last_print -= _fix_interval
            if time_since_last_print <= 0:
                time_since_last_print = print_interval
                self.dump_me()
            # logger.info(f"Sleeping for {SECS_TO_WAIT} secs before stabilizing again")

    async def fix_fingers(self):
        """
        Updates finger table to fix entries in case of change.
        """
        _fix_interval = int(dht_config["fix_interval"])
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
                    # # TODO: optimization need to check for correctness
                    for i in range(self._next + 1, len(self._fingers)):
                        __id = (self._numeric_id + (2 ** i)) % (2 ** len(self._fingers))
                        if between(
                            __id,
                            self._numeric_id,
                            succ["numeric_id"],
                            inclusive_right=False,
                            inclusive_left=False,
                        ):
                            self._fingers[i] = succ
            # print_table(self._fingers)

    @aiomas.expose
    def notify(self, n):
        """
        Notifies nodes which node n is now a predecessor of.
        Args:
            n (dict): The node notidying other nodes that it is their predecessor.
        """
        if not self._predecessor or between(
            n["numeric_id"],
            self._predecessor["numeric_id"],
            self._numeric_id,
            inclusive_left=False,
            inclusive_right=False,
        ):
            self._predecessor = n

    @aiomas.expose
    def save_key(self, key: str, value: str, ttl: int):
        """
        Stores key, val pair in the actual storage.
        Args:
            key (string): The key under which a vlue shall be stored.
            value (string): The value / data being stored.
            ttl (int): time to live. How long this should remain in the network.
        """
        logger.info(f"Saving key {key} => {value} in my storage.")
        return self._storage.put_key(key, value, ttl=ttl)

    @aiomas.expose
    async def put_key(self, key: str, value: str, ttl: int):
        """
        Generates multiple dht keys for each value for replication.
        Finds the node based on the key, where the value should be stored.
        Save it using save_key after a detination node is chosen.
        Args:
            key (string): The key under which a vlue shall be stored.
            value (string): The value / data being stored.
            ttl (int): time to live. How long this should remain in the network.
        """
        # generate multiple dht keys for each each
        for replica in range(1 + self._REPLICATION_COUNT):
            dht_key = generate_id(key)
            numeric_id = int(dht_key, 16)
            logger.warning(f"Putting Key: {key} - {dht_key} - {numeric_id}")
            found, next_node = await self.find_successor(numeric_id)
            if not found:
                return None
            logger.info(f"putting key {dht_key} on node {next_node['addr']}")
            await rpc_save_key(next_node, dht_key, value, ttl, ssl_ctx=self.client_ssl_ctx)
            key = dht_key

    @aiomas.expose
    async def find_key(self, key: str, ttl: int = 4, is_replica: bool = False):
        """
        checks current node for the value or deligates to appropriate succsorsself.
        Returns the value if it is stored on the ring.
        Args:
            key (string): The key under which a vlue shall be stored.
            value (string): The value / data being stored.
            ttl (int): time to live. How long this should remain in the network.
            is_replica (Boolean): Whether or not the current node is a replica.
        Returns:
            Boolean: Whether or not the value is found
            String: Value if one is found.
        """
        logger.debug(f"Finding key with TTL => {ttl} {key}")
        if ttl <= 0:
            return None
        search_cnt = 1 if is_replica else self._REPLICATION_COUNT + 1
        for idx in range(search_cnt):
            dht_key = generate_id(key)
            numeric_id = int(dht_key, 16)
            logger.warning(f"Getting Key: {key} - {dht_key} - {numeric_id}")
            found, value = self._find_key(dht_key)
            if found:
                return value
            found, node = await self.find_successor(numeric_id)
            if not found:
                continue
            logger.debug(f"Getting key from responsible node {node}")
            res = await rpc_get_key(node, key, ttl - 1, is_replica=idx > 0, ssl_ctx=self.client_ssl_ctx)
            if res:
                return res
            key = dht_key

    def _find_key(self, key: str):
        """
        A 'helper' function that returns the value if the key is store in the current node.
        Args:
            key (string): The key for whic the value is to be retrieved.
        Returns:
            Boolean: Whether or not the value is found
            String: Value if one is found.
        """
        value = self._storage.get_key(key)
        logger.info(f"finding key {key} => {value}")
        if value is not None:
            return True, value
        # get the succ responsible for the key
        return False, None

    @staticmethod
    @aiomas.expose
    def ping():
        return "pong"

    @aiomas.expose
    def get_all(self, node_id: int):
        """
        Gets all key, value pairs of the node with the given node_id
        Args:
            node_id (int): The id of the node.
        Returns:
            keys (list): The keys on the node as a list of strings
            values (list): Valus stored on the node as a list of string.
        """
        if not self._predecessor or not between(
            node_id,
            self._predecessor["numeric_id"],
            self._numeric_id,
            inclusive_right=False,
            inclusive_left=False,
        ):
            return [], []

        keys, values = self._storage.get_keys(self._predecessor["numeric_id"], node_id)
        self._storage.del_keys(keys)
        return keys, values

    def dump_me(self):
        """
        Used for debugging. prints a dump of all relevant node information.
        Prints the following for a node:
            *  my_data: node's `_addr`, `numeric_id`, `successor` and `predecessor`
            *  _successors: The successors of the current node.
            * _fingers: The finger table.
        """
        logger.debug("My data, succ and pred")
        my_data = [{"addr": self._addr, "id": self._id, "numeric_id": self._numeric_id}]
        my_data += [self._successor]
        my_data += [self._predecessor]
        print_table(my_data)

        logger.debug("My Successors")
        print_table(self._successors)

        logger.debug("My Fingers")
        print_table(self._fingers)
