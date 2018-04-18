import logging

from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated
from thrift.transport import TSocket, TTransport
from thrift.transport.TTransport import TTransportException

from elasticmem.kv import block_client, block_request_service
from elasticmem.kv.block_client import SingleBlockClient
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
        op_ids = [None if not self.blocks[i] else self.blocks[i].send_cmd(op, self.args[i]) for i in
                  range(len(self.blocks))]
        for i in range(len(self.blocks)):
            if op_ids[i] is not None:
                res.extend(self.blocks[i].recv_cmd(op_ids[i]))
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


# For Debugging only
class SingleServerKVClient:
    def __init__(self, data_status):
        self.file_info = data_status
        parts = data_status.data_blocks[0].block_names[0].split(':')
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(parts[0], int(parts[1])))
        self.protocol = TBinaryProtocolAccelerated(self.transport)
        self.client = block_request_service.Client(self.protocol)
        ex = None
        for i in range(3):
            try:
                self.transport.open()
            except TTransportException as e:
                ex = e
                continue
            except Exception:
                raise
            else:
                break
        else:
            raise TTransportException(ex.type, "Connection failed {}:{}: {}".format(parts[0], parts[1], ex.message))
        self.block_ids = [int(chain.block_names[0].split(':')[-1]) for chain in data_status.data_blocks]
        print(self.block_ids)
        self.blocks = [SingleBlockClient(self.transport, self.protocol, self.client, bid) for bid in self.block_ids]

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        if self.transport.isOpen():
            self.transport.close()

    def num_keys(self):
        return sum([int(block.num_keys()) for block in self.blocks])

    def pipeline_get(self, i=0):
        return SingleBlockPipelinedGet(self.blocks[i])

    def pipeline_put(self, i=0):
        return SingleBlockPipelinedPut(self.blocks[i])

    def pipeline_remove(self, i=0):
        return SingleBlockPipelinedRemove(self.blocks[i])

    def pipeline_update(self, i=0):
        return SingleBlockPipelinedUpdate(self.blocks[i])


class SingleBlockPipelinedBase(object):
    def __init__(self, block):
        self.block = block
        self.args = []

    def reset(self):
        self.args = []

    def _send_execute(self, op):
        op_id = self.block.send_cmd(op, self.args)
        self.reset()
        return op_id

    def recv_execute(self, op_id):
        return self.block.recv_cmd(op_id)

    def _execute(self, op):
        op_id = self._send_execute(op)
        return self.recv_execute(op_id)


class SingleBlockPipelinedGet(SingleBlockPipelinedBase):
    def __init__(self, block):
        super(SingleBlockPipelinedGet, self).__init__(block)

    def get(self, key):
        self.args.append(key)

    def send_execute(self):
        return self._send_execute(KVOps.get)

    def execute(self):
        return self._execute(KVOps.get)


class SingleBlockPipelinedPut(SingleBlockPipelinedBase):
    def __init__(self, block):
        super(SingleBlockPipelinedPut, self).__init__(block)

    def put(self, key, value):
        self.args.append(key)
        self.args.append(value)

    def send_execute(self):
        return self._send_execute(KVOps.put)

    def execute(self):
        return self._execute(KVOps.put)


class SingleBlockPipelinedRemove(SingleBlockPipelinedBase):
    def __init__(self, block):
        super(SingleBlockPipelinedRemove, self).__init__(block)

    def remove(self, key):
        self.args.append(key)

    def send_execute(self):
        return self._send_execute(KVOps.remove)

    def execute(self):
        return self._execute(KVOps.remove)


class SingleBlockPipelinedUpdate(SingleBlockPipelinedBase):
    def __init__(self, block):
        super(SingleBlockPipelinedUpdate, self).__init__(block)

    def update(self, key, value):
        self.args.append(key)
        self.args.append(value)

    def send_execute(self):
        return self._send_execute(KVOps.update)

    def execute(self):
        return self._execute(KVOps.update)
