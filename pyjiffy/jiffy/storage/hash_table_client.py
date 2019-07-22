import logging
from bisect import bisect_right

from jiffy.directory.directory_client import ReplicaChain, StorageMode
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
        response = b(response)
        while response.startswith(b('!exporting')):
            chain = ReplicaChain([bytes_to_str(x) for x in response[1:].split(b('!'))[1:]], 0, 0,
                                 rpc_storage_mode.rpc_in_memory)
            response = \
                ReplicaChainClient(self.fs, self.path, self.client_cache, chain,
                                   HashTableOps.op_types).run_command_redirected(args)[0]
        if response == b('!block_moved'):
            self.refresh()
            return None
        return response

    def _handle_redirects(self, args, responses):
        n_ops = len(responses)
        n_op_args = int(len(args) / n_ops)
        for i in range(n_ops):
            response = b(responses[i])
            while response.startswith(b('!exporting')):
                chain = ReplicaChain([bytes_to_str(x) for x in response[1:].split(b('!'))[1:]], 0, 0,
                                     StorageMode.in_memory)
                op_args = args[i * n_op_args: (i + 1) * n_op_args]
                response = \
                    ReplicaChainClient(self.fs, self.path, self.client_cache, chain,
                                       HashTableOps.op_types).run_command_redirected(op_args)[0]
            if response == "!block_moved":
                self.refresh()
                return None
            responses[i] = response
        return responses

    def put(self, key, value):
        args = [HashTableOps.put, key, value]
        response = None
        while response is None:
            response = self.blocks[self.block_id(key)].run_command(args)[0]
            response = self._handle_redirect(args, response)
        return response

    def get(self, key):
        args = [HashTableOps.get, key]
        response = None
        while response is None:
            response = self.blocks[self.block_id(key)].run_command(args)[0]
            response = self._handle_redirect(args, response)
        return response

    def exists(self, key):
        args = [HashTableOps.exists, key]
        response = None
        while response is None:
            response = self.blocks[self.block_id(key)].run_command(args)[0]
            response = self._handle_redirect(args, response)
        return response == b('true')

    def update(self, key, value):
        args = [HashTableOps.update, key, value]
        response = None
        while response is None:
            response = self.blocks[self.block_id(key)].run_command(args)[0]
            response = self._handle_redirect(args, response)
        return response

    def upsert(self, key, value):
        args = [HashTableOps.upsert, key, value]
        response = None
        while response is None:
            response = self.blocks[self.block_id(key)].run_command(args)[0]
            response = self._handle_redirect(args, response)
        return response

    def remove(self, key):
        args = [HashTableOps.remove, key]
        response = None
        while response is None:
            response = self.blocks[self.block_id(key)].run_command(args)[0]
            response = self._handle_redirect(args, response)
        return response

    def block_id(self, key):
        i = bisect_right(self.slots, crc.crc16(encode(key)))
        if i:
            return i - 1
        raise ValueError
