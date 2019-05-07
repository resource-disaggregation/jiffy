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
    class LockedClient:
        def __init__(self, parent):
            self.parent = parent
            self.blocks = [block.lock() for block in self.parent.blocks]
            self.redirect_blocks = [None for _ in self.blocks]
            self.locked_redirect_blocks = [None for _ in self.blocks]
            self.new_blocks = [None for _ in self.blocks]
            for i in range(len(self.blocks)):
                if self.blocks[i].is_redirecting():
                    new_block = True
                    for j in range(len(self.blocks)):
                        if self.blocks[i].get_redirect_chain() == self.blocks[j].get_chain():
                            new_block = False
                            self.redirect_blocks[i] = self.parent.blocks[j]
                            self.locked_redirect_blocks[i] = self.blocks[j]
                            break

                    if new_block:
                        self.redirect_blocks[i] = ReplicaChainClient(self.parent.fs,
                                                                     self.parent.path,
                                                                     self.parent.client_cache,
                                                                     self.blocks[i].get_redirect_chain())
                        self.locked_redirect_blocks[i] = self.redirect_blocks[i].lock()
                        self.new_blocks[i] = self.locked_redirect_blocks[i]

        def _handle_redirect(self, cmd_id, args, response):
            response = b(response)
            while response.startswith(b('!exporting')):
                chain = [bytes_to_str(x) for x in response[1:].split(b('!'))[1:]]
                found = False
                for i in range(len(self.blocks)):
                    client_chain = self.blocks[i].get_chain()
                    if client_chain == chain:
                        found = True
                        response = self.blocks[i].run_command_redirected(cmd_id, args)[0]
                        break
                if not found:
                    response = ReplicaChainClient(self.parent.fs, self.parent.path, self.parent.client_cache,
                                                  chain).run_command_redirected(cmd_id, args)[0]
            return response

        def _handle_redirects(self, cmd_id, args, responses):
            n_ops = len(responses)
            n_op_args = int(len(args) / n_ops)
            for i in range(n_ops):
                response = b(responses[i])
                while response.startswith(b('!exporting')):
                    chain = ReplicaChain([bytes_to_str(x) for x in response[1:].split(b('!'))[1:]], 0, 0, 0)
                    op_args = args[i * n_op_args: (i + 1) * n_op_args]
                    found = False
                    for j in range(len(self.blocks)):
                        client_chain = self.parent.blocks[j].get_chain()
                        if client_chain == chain:
                            found = True
                            response = self.blocks[j].run_command_redirected(cmd_id, op_args)[0]
                            break
                    if not found:
                        response = \
                            ReplicaChainClient(self.parent.fs, self.parent.path, self.parent.client_cache,
                                               chain).run_command_redirected(cmd_id, op_args)[0]
                responses[i] = response
            return responses

        def unlock(self):
            for i in range(len(self.blocks)):
                self.blocks[i].unlock()
                if self.new_blocks[i] is not None:
                    self.new_blocks[i].unlock()

        def put(self, key, value):
            args = [key, value]
            response = self.blocks[self.parent.block_id(key)].run_command(HashTableOps.locked_put, args)[0]
            return self._handle_redirect(HashTableOps.locked_put, args, response)

        def get(self, key):
            args = [key]
            response = self.blocks[self.parent.block_id(key)].run_command(HashTableOps.locked_get, args)[0]
            return self._handle_redirect(HashTableOps.locked_get, args, response)

        def update(self, key, value):
            args = [key, value]
            response = self.blocks[self.parent.block_id(key)].run_command(HashTableOps.locked_update, args)[0]
            return self._handle_redirect(HashTableOps.locked_update, args, response)

        def remove(self, key):
            args = [key]
            response = self.blocks[self.parent.block_id(key)].run_command(HashTableOps.locked_remove, args)[0]
            return self._handle_redirect(HashTableOps.locked_remove, args, response)

        def multi_put(self, args):
            response = self.parent._batch_command(HashTableOps.locked_put, args, 2)
            return self._handle_redirects(HashTableOps.locked_put, args, response)

        def multi_get(self, args):
            response = self.parent._batch_command(HashTableOps.locked_get, args, 1)
            return self._handle_redirects(HashTableOps.locked_get, args, response)

        def multi_update(self, args):
            response = self.parent._batch_command(HashTableOps.locked_update, args, 2)
            return self._handle_redirects(HashTableOps.locked_update, args, response)

        def multi_remove(self, args):
            response = self.parent._batch_command(HashTableOps.locked_remove, args, 1)
            return self._handle_redirects(HashTableOps.locked_remove, args, response)

        def num_keys(self):
            for i in range(len(self.blocks)):
                self.blocks[i].send_command(HashTableOps.num_keys, [])
                if self.new_blocks[i] is not None:
                    self.new_blocks[i].send_command(HashTableOps.num_keys, [])

            n = 0
            for i in range(len(self.blocks)):
                n += int(self.blocks[i].recv_response()[0])
                if self.new_blocks[i] is not None:
                    n += int(self.new_blocks[i].recv_response()[0])
            return n

    def __init__(self, fs, path, data_status, timeout_ms=1000):
        self.fs = fs
        self.path = path
        self.client_cache = BlockClientCache(timeout_ms)
        self.file_info = data_status
        self.blocks = [ReplicaChainClient(self.fs, self.path, self.client_cache, chain) for chain in
                       data_status.data_blocks]
        self.slots = [int(chain.name.split('_')[0]) for chain in self.file_info.data_blocks]

    def refresh(self):
        self.file_info = self.fs.dstatus(self.path)
        logging.info("Refreshing block mappings to {}".format(self.file_info.data_blocks))
        self.blocks = [ReplicaChainClient(self.fs, self.path, self.client_cache, chain) for chain in
                       self.file_info.data_blocks]
        self.slots = [int(chain.name.split('_')[0]) for chain in self.file_info.data_blocks]

    def lock(self):
        return self.LockedClient(self)

    def _handle_redirect(self, cmd_id, args, response):
        response = b(response)
        while response.startswith(b('!exporting')):
            chain = ReplicaChain([bytes_to_str(x) for x in response[1:].split(b('!'))[1:]], 0, 0,
                                 rpc_storage_mode.rpc_in_memory)
            response = \
                ReplicaChainClient(self.fs, self.path, self.client_cache, chain).run_command_redirected(cmd_id, args)[0]
        if response == b('!block_moved'):
            self.refresh()
            return None
        return response

    def _handle_redirects(self, cmd_id, args, responses):
        n_ops = len(responses)
        n_op_args = int(len(args) / n_ops)
        for i in range(n_ops):
            response = b(responses[i])
            while response.startswith(b('!exporting')):
                chain = ReplicaChain([bytes_to_str(x) for x in response[1:].split(b('!'))[1:]], 0, 0,
                                     StorageMode.in_memory)
                op_args = args[i * n_op_args: (i + 1) * n_op_args]
                response = \
                    ReplicaChainClient(self.fs, self.path, self.client_cache, chain).run_command_redirected(cmd_id,
                                                                                                            op_args)[0]
            if response == "!block_moved":
                self.refresh()
                return None
            responses[i] = response
        return responses

    def _batch_command(self, op, args, args_per_op):
        if len(args) % args_per_op != 0:
            raise RuntimeError("Incorrect number of arguments")

        num_ops = int(len(args) / args_per_op)
        block_args = [[] for _ in self.blocks]
        positions = [[] for _ in self.blocks]

        for i in range(num_ops):
            bid = self.block_id(args[i * args_per_op])
            block_args[bid].extend(args[i * args_per_op: (i + 1) * args_per_op])
            positions[bid].append(i)

        for i in range(len(self.blocks)):
            if block_args[i]:
                self.blocks[i].send_command(op, block_args[i])

        responses = [None for _ in range(num_ops)]
        for i in range(len(self.blocks)):
            if block_args[i]:
                response = self.blocks[i].recv_response()
                for j in range(len(response)):
                    responses[positions[i][j]] = response[j]

        return responses

    def put(self, key, value):
        args = [key, value]
        response = None
        while response is None:
            response = self.blocks[self.block_id(key)].run_command(HashTableOps.put, args)[0]
            response = self._handle_redirect(HashTableOps.put, args, response)
        return response

    def get(self, key):
        args = [key]
        response = None
        while response is None:
            response = self.blocks[self.block_id(key)].run_command(HashTableOps.get, args)[0]
            response = self._handle_redirect(HashTableOps.get, args, response)
        return response

    def exists(self, key):
        args = [key]
        response = None
        while response is None:
            response = self.blocks[self.block_id(key)].run_command(HashTableOps.exists, args)[0]
            response = self._handle_redirect(HashTableOps.exists, args, response)
        return response == b'true'

    def update(self, key, value):
        args = [key, value]
        response = None
        while response is None:
            response = self.blocks[self.block_id(key)].run_command(HashTableOps.update, args)[0]
            response = self._handle_redirect(HashTableOps.update, args, response)
        return response

    def remove(self, key):
        args = [key]
        response = None
        while response is None:
            response = self.blocks[self.block_id(key)].run_command(HashTableOps.remove, args)[0]
            response = self._handle_redirect(HashTableOps.remove, args, response)
        return response

    def multi_put(self, args):
        response = None
        while response is None:
            response = self._batch_command(HashTableOps.put, args, 2)
            response = self._handle_redirects(HashTableOps.put, args, response)
        return response

    def multi_get(self, args):
        response = None
        while response is None:
            response = self._batch_command(HashTableOps.get, args, 1)
            response = self._handle_redirects(HashTableOps.get, args, response)
        return response

    def multi_exists(self, args):
        response = None
        while response is None:
            response = self._batch_command(HashTableOps.exists, args, 1)
            response = self._handle_redirects(HashTableOps.exists, args, response)
        return [r == b'true' for r in response]

    def multi_update(self, args):
        response = None
        while response is None:
            response = self._batch_command(HashTableOps.update, args, 2)
            response = self._handle_redirects(HashTableOps.update, args, response)
        return response

    def multi_remove(self, args):
        response = None
        while response is None:
            response = self._batch_command(HashTableOps.remove, args, 1)
            response = self._handle_redirects(HashTableOps.remove, args, response)
        return response

    def block_id(self, key):
        i = bisect_right(self.slots, crc.crc16(encode(key)))
        if i:
            return i - 1
        raise ValueError
