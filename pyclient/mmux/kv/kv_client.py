import logging
from bisect import bisect_right

from mmux.kv import crc
from mmux.kv.block_client import BlockChainClient, BlockClientCache
from mmux.kv.compat import b, unicode, bytes, long, basestring, char_to_byte, bytes_to_str
from mmux.kv.kv_ops import KVOps


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


class KVClient:
    def __init__(self, fs, path, data_status):
        self.fs = fs
        self.path = path
        self.client_cache = BlockClientCache()
        self.file_info = data_status
        self.blocks = [BlockChainClient(self.client_cache, chain.block_names) for chain in data_status.data_blocks]
        self.slots = [chain.slot_range[0] for chain in data_status.data_blocks]

    def refresh(self):
        self.file_info = self.fs.dstatus(self.path)
        logging.info("Refreshing block mappings to {}".format(self.file_info.data_blocks))
        self.blocks = [BlockChainClient(self.client_cache, chain.block_names) for chain in self.file_info.data_blocks]
        self.slots = [chain.slot_range[0] for chain in self.file_info.data_blocks]

    def _parse_response(self, r):
        response = b(r)
        if response[0] == char_to_byte('!'):  # Actionable response
            if response[1:] == b('ok'):
                return "ok"
            elif response[1:] == b('key_not_found'):
                return None
            elif response[1:] == b('duplicate_key'):
                raise ValueError('Key already exists')
            elif response[1:] == b('args_error'):
                raise ValueError('Incorrect arguments')
            elif response[1:] == b('block_moved'):
                self.refresh()
                raise RedoError
            elif response[1:].startswith(b('exporting')):
                block_names = [bytes_to_str(x) for x in response[1:].split(b('!'))[1:]]
                raise RedirectError('Redirect to {}'.format(block_names), block_names)
        return r

    def put(self, key, value):
        try:
            return self._parse_response(self.blocks[self.block_id(key)].put(key, value))
        except RedoError:
            return self._parse_response(self.blocks[self.block_id(key)].put(key, value))
        except RedirectError as e:
            return self._parse_response(BlockChainClient(self.client_cache, e.blocks).redirected_put(key, value))

    def get(self, key):
        try:
            return self._parse_response(self.blocks[self.block_id(key)].get(key))
        except RedoError:
            return self._parse_response(self.blocks[self.block_id(key)].get(key))
        except RedirectError as e:
            return self._parse_response(BlockChainClient(self.client_cache, e.blocks).redirected_get(key))

    def exists(self, key):
        try:
            return self._parse_response(self.blocks[self.block_id(key)].exists(key)) == b'true'
        except RedoError:
            return self._parse_response(self.blocks[self.block_id(key)].exists(key)) == b'true'
        except RedirectError as e:
            return self._parse_response(BlockChainClient(self.client_cache, e.blocks).redirected_exists(key)) == b'true'

    def update(self, key, value):
        try:
            return self._parse_response(self.blocks[self.block_id(key)].update(key, value))
        except RedoError:
            return self._parse_response(self.blocks[self.block_id(key)].update(key, value))
        except RedirectError as e:
            return self._parse_response(BlockChainClient(self.client_cache, e.blocks).redirected_update(key, value))

    def remove(self, key):
        try:
            return self._parse_response(self.blocks[self.block_id(key)].remove(key))
        except RedoError:
            return self._parse_response(self.blocks[self.block_id(key)].remove(key))
        except RedirectError as e:
            return self._parse_response(BlockChainClient(self.client_cache, e.blocks).redirected_remove(key))

    def pipeline(self):
        return PipelinedClient(self.path, self.blocks, self.slots)

    def block_id(self, key):
        i = bisect_right(self.slots, crc.crc16(encode(key)))
        if i:
            return i - 1
        raise ValueError


def op_index(ops, args, op):
    try:
        idx = ops.index(op)
    except ValueError:
        idx = len(ops)
        ops.append(op)
        args.append([])
    return idx


class PipelinedClient:
    def __init__(self, path, blocks, slots):
        self.path = path
        self.blocks = blocks
        self.slots = slots
        self.accessor_ops = [[] for _ in self.blocks]
        self.accessor_args = [[] for _ in self.blocks]
        self.mutator_ops = [[] for _ in self.blocks]
        self.mutator_args = [[] for _ in self.blocks]

    def reset(self):
        self.accessor_ops = [[] for _ in self.blocks]
        self.accessor_args = [[] for _ in self.blocks]
        self.mutator_ops = [[] for _ in self.blocks]
        self.mutator_args = [[] for _ in self.blocks]

    def _add_accessor(self, op, bid, *args):
        idx = op_index(self.accessor_ops[bid], self.accessor_args[bid], op)
        self.accessor_args[bid][idx].extend(args)

    def _add_mutator(self, op, bid, *args):
        idx = op_index(self.mutator_ops[bid], self.mutator_args[bid], op)
        self.mutator_args[bid][idx].extend(args)

    def put(self, key, value):
        self._add_mutator(KVOps.put, self.block_id(key), key, value)

    def get(self, key):
        self._add_accessor(KVOps.get, self.block_id(key), key)

    def exists(self, key):
        self._add_accessor(KVOps.exists, self.block_id(key), key)

    def update(self, key, value):
        self._add_mutator(KVOps.update, self.block_id(key), key, value)

    def remove(self, key):
        self._add_mutator(KVOps.remove, self.block_id(key), key)

    def block_id(self, key):
        i = bisect_right(self.slots, crc.crc16(encode(key)))
        if i:
            return i - 1
        raise ValueError

    def send_accessor(self, i):
        return None if not self.accessor_ops[i] else self.blocks[i].send_cmd_tail(self.accessor_ops[i],
                                                                                  self.accessor_args[i])

    def send_mutator(self, i):
        return None if not self.mutator_ops[i] else self.blocks[i].send_cmd_head(self.mutator_ops[i],
                                                                                 self.mutator_args[i])

    def execute(self):
        res = []
        if self.accessor_ops:
            accessor_op_seqs = [self.send_accessor(i) for i in range(len(self.blocks))]
        else:
            accessor_op_seqs = []

        if self.mutator_ops:
            mutator_op_seqs = [self.send_mutator(i) for i in range(len(self.blocks))]
        else:
            mutator_op_seqs = []

        for i in range(len(self.blocks)):
            if accessor_op_seqs and accessor_op_seqs[i] is not None:
                res.extend(self.blocks[i].recv_cmd(accessor_op_seqs[i])[0])
            if mutator_op_seqs and mutator_op_seqs[i] is not None:
                res.extend(self.blocks[i].recv_cmd(mutator_op_seqs[i])[0])
        self.reset()
        return res
