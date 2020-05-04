import math
import time
import copy
from bisect import bisect_right

from jiffy.directory.directory_client import ReplicaChain
from jiffy.directory.ttypes import rpc_storage_mode
from jiffy.storage import crc
from jiffy.storage.command import CommandType
from jiffy.storage.compat import b, unicode, bytes, long, basestring, bytes_to_str
from jiffy.storage.data_structure_client import DataStructureClient
from jiffy.storage.replica_chain_client import ReplicaChainClient


class HashTableOps:
    exists = b('exists')
    get = b('get')
    put = b('put')
    remove = b('remove')
    update = b('update')
    upsert = b('upsert')

    op_types = {exists: CommandType.accessor,
                get: CommandType.accessor,
                put: CommandType.mutator,
                remove: CommandType.mutator,
                update: CommandType.accessor,
                upsert: CommandType.mutator}


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


class HashTable(DataStructureClient):
    def __init__(self, fs, path, block_info, timeout_ms):
        super(HashTable, self).__init__(fs, path, block_info, HashTableOps.op_types, timeout_ms)
        self.slots = [int(chain.name.split('_')[0]) for chain in self.block_info.data_blocks]

    def _refresh(self):
        super(HashTable, self)._refresh()
        self.slots = [int(chain.name.split('_')[0]) for chain in self.block_info.data_blocks]

    def _handle_redirect(self, args, response):
        while b(response[0]) == b('!exporting'):
            args_copy = copy.deepcopy(args)
            if args[0] == b("update") or args[0] == b("upsert"):
                args_copy += [response[2], response[3]]
            block_ids = [bytes_to_str(x) for x in response[1].split(b('!'))]
            chain = ReplicaChain(block_ids, 0, 0, rpc_storage_mode.rpc_in_memory)
            while True:
                response = ReplicaChainClient(self.fs, self.path, self.client_cache, chain,
                                            HashTableOps.op_types).run_command_redirected(args_copy)
                if b(response[0]) != b("!redo"):
                    break
        if b(response[0]) == b('!block_moved'):
            self._refresh()
            return None
        if b(response[0]) == b('!full'):
            time.sleep(0.001 * math.pow(2, self.redo_times))
            self.redo_times += 1
            return None
        if b(response[0]) == b("!redo"):
            return None
        return response

    def put(self, key, value):
        self._run_repeated([HashTableOps.put, key, value])

    def get(self, key):
        return self._run_repeated([HashTableOps.get, key])[1]

    def exists(self, key):
        try:
            self._run_repeated([HashTableOps.exists, key])[0]
        except:
            return False
        return True

    def update(self, key, value):
        return self._run_repeated([HashTableOps.update, key, value])[0]

    def upsert(self, key, value):
        self._run_repeated([HashTableOps.upsert, key, value])

    def remove(self, key):
        return self._run_repeated([HashTableOps.remove, key])[0]

    def _block_id(self, args):
        i = bisect_right(self.slots, crc.crc16(encode(args[1])))
        if i:
            return i - 1
        raise ValueError
