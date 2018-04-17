import logging

from elasticmem.kv import block_client
from elasticmem.kv.kv_ops import KVOps


class KVClient:
    def __init__(self, path, data_status, chain_failure_cb, hash_fn=hash):
        self.path = path
        self.file_info = data_status
        self.blocks = [block_client.BlockChainClient(chain.block_names) for chain in data_status.data_blocks]
        self.chain_failure_cb_ = chain_failure_cb
        self.hash_fn_ = hash_fn

    def disconnect(self):
        for c in self.blocks:
            c.disconnect()

    def pipeline_put(self):
        return PipelinedPut(self.path, self.blocks, self.chain_failure_cb_, self.hash_fn_)

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
        return PipelinedGet(self.path, self.blocks, self.chain_failure_cb_, self.hash_fn_)

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
        return PipelinedUpdate(self.path, self.blocks, self.chain_failure_cb_, self.hash_fn_)

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
        return PipelinedRemove(self.path, self.blocks, self.chain_failure_cb_, self.hash_fn_)

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
        return self.hash_fn_(key) % len(self.blocks)


class PipelinedBase(object):
    def __init__(self, path, blocks, chain_failure_cb, hash_fn):
        self.path = path
        self.blocks = blocks
        self.chain_failure_cb_ = chain_failure_cb
        self.hash_fn_ = hash_fn
        self.args = [[] for _ in self.blocks]

    def reset(self):
        self.args = [[] for _ in self.blocks]

    def block_id(self, key):
        return self.hash_fn_(key) % len(self.blocks)

    def _execute(self, op):
        res = []
        for i in range(len(self.blocks)):
            if len(self.args[i]):
                res.extend(self.blocks[i].run_command(op, self.args[i]))
        self.reset()
        return res


class PipelinedGet(PipelinedBase):
    def __init__(self, path, blocks, chain_failure_cb, hash_fn):
        super(PipelinedGet, self).__init__(path, blocks, chain_failure_cb, hash_fn)

    def get(self, key):
        self.args[self.block_id(key)].append(key)

    def execute(self):
        return self._execute(KVOps.get)


class PipelinedPut(PipelinedBase):
    def __init__(self, path, blocks, chain_failure_cb, hash_fn):
        super(PipelinedPut, self).__init__(path, blocks, chain_failure_cb, hash_fn)

    def put(self, key, value):
        bid = self.block_id(key)
        self.args[bid].append(key)
        self.args[bid].append(value)

    def execute(self):
        return self._execute(KVOps.put)


class PipelinedRemove(PipelinedBase):
    def __init__(self, path, blocks, chain_failure_cb, hash_fn):
        super(PipelinedRemove, self).__init__(path, blocks, chain_failure_cb, hash_fn)

    def remove(self, key):
        self.args[self.block_id(key)].append(key)

    def execute(self):
        return self._execute(KVOps.remove)


class PipelinedUpdate(PipelinedBase):
    def __init__(self, path, blocks, chain_failure_cb, hash_fn):
        super(PipelinedUpdate, self).__init__(path, blocks, chain_failure_cb, hash_fn)

    def update(self, key, value):
        bid = self.block_id(key)
        self.args[bid].append(key)
        self.args[bid].append(value)

    def execute(self):
        return self._execute(KVOps.update)
