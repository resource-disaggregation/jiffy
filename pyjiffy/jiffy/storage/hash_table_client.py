import logging
from bisect import bisect_right

import math
import time

from jiffy.directory.directory_client import ReplicaChain
from jiffy.directory.ttypes import rpc_storage_mode
from jiffy.storage import crc
from jiffy.storage.block_client import BlockClientCache
from jiffy.storage.compat import b, unicode, bytes, long, basestring, bytes_to_str
from jiffy.storage.hash_table_ops import HashTableOps
from jiffy.storage.replica_chain_client import ReplicaChainClient


def encode(value):
    if isinstance(value, bytes):
        return value
    elif isinstance(value, (int, long)):
        value = b(str(value))
    elif isinstance(value, float):
        value = b(repr(value))
    elif not isinstance(value, basestring):
        value = unicode(value)
    if isinstance(value, unicode):
        value = value.encode()
    return value


class RedoError(Exception):
    pass


class RedirectError(Exception):
    def __init__(self, message, blocks):
        super(RedirectError, self).__init__(message)
        self.blocks = blocks


class HashTableClient:
    def __init__(self, fs, path, data_status, timeout_ms=1000):
        self.redo_times = 0
        self.fs = fs
        self.path = path
        self.client_cache = BlockClientCache(timeout_ms)
        self.file_info = data_status
        self.blocks = [ReplicaChainClient(self.fs, self.path, self.client_cache, chain, HashTableOps.op_types) for chain
                       in data_status.data_blocks]
        self.slots = [int(chain.name.split('_')[0]) for chain in self.file_info.data_blocks]

    def refresh(self):
        self.file_info = self.fs.dstatus(self.path)
        logging.info("Refreshing block mappings to {}".format(self.file_info.data_blocks))
        self.blocks = [ReplicaChainClient(self.fs, self.path, self.client_cache, chain, HashTableOps.op_types) for chain
                       in self.file_info.data_blocks]
        self.slots = [int(chain.name.split('_')[0]) for chain in self.file_info.data_blocks]

    def _handle_redirect(self, args, response):
        while b(response[0]) == b('!exporting'):
            chain = ReplicaChain([bytes_to_str(x) for x in response[1].split(b('!'))], 0, 0,
                                 rpc_storage_mode.rpc_in_memory)
            response = ReplicaChainClient(self.fs, self.path, self.client_cache, chain,
                                          HashTableOps.op_types).run_command_redirected(args)
        if b(response[0]) == b('!block_moved'):
            self.refresh()
            return None
        if b(response[0]) == b('!full'):
            time.sleep(0.001 * math.pow(2, self.redo_times))
            self.redo_times += 1
            return None
        return response

    def run_repeated(self, key, args):
        response = None
        while response is None:
            response = self.blocks[self.block_id(key)].run_command(args)
            response = self._handle_redirect(args, response)
        self.redo_times = 0
        if b(response[0]) != b('!ok'):
            raise KeyError(response[0])
        return response

    def put(self, key, value):
        self.run_repeated(key, [HashTableOps.put, key, value])

    def get(self, key):
        return self.run_repeated(key, [HashTableOps.get, key])[1]

    def exists(self, key):
        return self.run_repeated(key, [HashTableOps.exists, key])[1] == b('true')

    def update(self, key, value):
        return self.run_repeated(key, [HashTableOps.update, key, value])[1]

    def upsert(self, key, value):
        self.run_repeated(key, [HashTableOps.upsert, key, value])

    def remove(self, key):
        return self.run_repeated(key, [HashTableOps.remove, key])[1]

    def block_id(self, key):
        i = bisect_right(self.slots, crc.crc16(encode(key)))
        if i:
            return i - 1
        raise ValueError
