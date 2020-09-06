import aiomas
from .helpers import generate_id
from config.config import dht_config


class ChordServer:
    router = aiomas.rpc.Service()

    def __init__(self, host, port):
        super().__init__()
        self._id = generate_id(f"{host}:{port}")
        self.numeric_id = int(self._id, 16)

        self.FINGER_TABLE_SZ = int(dht_config["finger_table_sz"])
        self.DHT_RING_SZ = 2 ** self.FINGER_TABLE_SZ

        self.finger_table = [{"node": None, "start": -1}] * self.FINGER_TABLE_SZ
        self.successor = None
        self.predecessor = None

    def __init_fingertable(self, bootstrap_addr):
        pass

    def __update_others(self):
        pass

    def find_succ(self, _id):
        if _id >= self.numeric_id and _id <= self.successor:
            return self
        else:
            # recursive call to find n` and then ask for the succ
            # TODO: make rpc call
            n_node = self.closest_preceding_node(_id)

    def closest_preceding_node(self, _id):
        for k in range(self.FINGER_TABLE_SZ - 1, 0, -1):
            node_id = self.finger_table[k].node.numeric_id
            if node_id >= self.numeric_id and node_id <= _id:
                return self.finger_table[k].node
        return self

    @aiomas.expose
    def join(self, bootstrap_addr=None):
        if not bootstrap_addr:
            # create a new chord dht
            for k in range(self.FINGER_TABLE_SZ - 1):
                self.finger_table[k] = {
                    "node": self,  # need to change probably
                    "start": (self.numeric_id + 2 ** k) % self.DHT_RING_SZ,
                }
            self.successor = self
            self.predecessor = self
        else:
            # network already exists
            # update my fingertable via bootstrap node
            self.__init_fingertable(bootstrap_addr)
            # update other nodes fingertables about myself
            self.__update_others()
            # TODO:: move keys to myself
        return self.finger_table
