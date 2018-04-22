import logging
from bisect import bisect_right

from elasticmem.kv import block_client, crc
from elasticmem.kv.kv_ops import KVOps
from elasticmem.kv.compat import b, unicode, bytes, long, basestring


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


class KVClient:
    def __init__(self, fs, path, data_status, chain_failure_cb):
        self.fs = fs
        self.path = path
        self.file_info = data_status
        self.blocks = [block_client.BlockChainClient(chain.block_names) for chain in data_status.data_blocks]
        self.slots = [chain.slot_range[0] for chain in data_status.data_blocks]
        self.chain_failure_cb_ = chain_failure_cb

    def disconnect(self):
        for c in self.blocks:
            c.disconnect()

    def pipeline_put(self):
        return PipelinedPut(self.path, self.blocks, self.slots, self.chain_failure_cb_)

    def send_put(self, key, value):
        bid = self.block_id(key)
        return bid, self.blocks[bid].send_put(key, value)

    def put(self, key, value):
        try:
            return self.blocks[self.block_id(key)].put(key, value)
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def pipeline_get(self):
        return PipelinedGet(self.path, self.blocks, self.slots, self.chain_failure_cb_)

    def send_get(self, key):
        bid = self.block_id(key)
        return bid, self.blocks[bid].send_get(key)

    def get(self, key):
        try:
            return self.blocks[self.block_id(key)].get(key)
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def num_keys(self):
        return sum([int(block.num_keys()) for block in self.blocks])

    def pipeline_update(self):
        return PipelinedUpdate(self.path, self.blocks, self.slots, self.chain_failure_cb_)

    def send_update(self, key, value):
        bid = self.block_id(key)
        return bid, self.blocks[bid].send_update(key, value)

    def update(self, key, value):
        try:
            return self.blocks[self.block_id(key)].update(key, value)
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def pipeline_remove(self):
        return PipelinedRemove(self.path, self.blocks, self.slots, self.chain_failure_cb_)

    def send_remove(self, key):
        bid = self.block_id(key)
        return bid, self.blocks[bid].send_remove(key)

    def remove(self, key):
        try:
            return self.blocks[self.block_id(key)].remove(key)
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def recv_response(self, op_seq):
        return self.blocks[op_seq[0]].recv_response(op_seq[1])

    def recv_responses(self, op_seqs):
        return [self.recv_response(op_seq) for op_seq in op_seqs]

    def block_id(self, key):
        i = bisect_right(self.slots, crc.crc16(encode(key)))
        if i:
            return i - 1
        raise ValueError


class PipelinedBase(object):
    def __init__(self, path, blocks, slots, chain_failure_cb):
        self.path = path
        self.blocks = blocks
        self.slots = slots
        self.chain_failure_cb_ = chain_failure_cb
        self.args = [[] for _ in self.blocks]

    def reset(self):
        self.args = [[] for _ in self.blocks]

    def block_id(self, key):
        i = bisect_right(self.slots, crc.crc16(encode(key)))
        if i:
            return i - 1
        raise ValueError

    def _execute(self, op):
        res = []
        op_ids = [None if not self.blocks[i] else self.blocks[i].send_cmd(op, self.args[i]) for i in
                  range(len(self.blocks))]
        for i in range(len(self.blocks)):
            if op_ids[i] is not None:
                res.extend(self.blocks[i].recv_cmd(op_ids[i]))
        self.reset()
        return res


class PipelinedGet(PipelinedBase):
    def __init__(self, path, blocks, slots, chain_failure_cb):
        super(PipelinedGet, self).__init__(path, blocks, slots, chain_failure_cb)

    def get(self, key):
        self.args[self.block_id(key)].append(key)

    def execute(self):
        return self._execute(KVOps.get)


class PipelinedPut(PipelinedBase):
    def __init__(self, path, blocks, slots, chain_failure_cb):
        super(PipelinedPut, self).__init__(path, blocks, slots, chain_failure_cb)

    def put(self, key, value):
        bid = self.block_id(key)
        self.args[bid].append(key)
        self.args[bid].append(value)

    def execute(self):
        return self._execute(KVOps.put)


class PipelinedRemove(PipelinedBase):
    def __init__(self, path, blocks, slots, chain_failure_cb):
        super(PipelinedRemove, self).__init__(path, blocks, slots, chain_failure_cb)

    def remove(self, key):
        self.args[self.block_id(key)].append(key)

    def execute(self):
        return self._execute(KVOps.remove)


class PipelinedUpdate(PipelinedBase):
    def __init__(self, path, blocks, slots, chain_failure_cb):
        super(PipelinedUpdate, self).__init__(path, blocks, slots, chain_failure_cb)

    def update(self, key, value):
        bid = self.block_id(key)
        self.args[bid].append(key)
        self.args[bid].append(value)

    def execute(self):
        return self._execute(KVOps.update)
