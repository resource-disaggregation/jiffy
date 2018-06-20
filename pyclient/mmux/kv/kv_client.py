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
    def __init__(self, fs, path, data_status, chain_failure_cb):
        self.fs = fs
        self.path = path
        self.client_cache = BlockClientCache()
        self.file_info = data_status
        self.blocks = [BlockChainClient(self.client_cache, chain.block_names) for chain in data_status.data_blocks]
        self.slots = [chain.slot_range[0] for chain in data_status.data_blocks]
        self.chain_failure_cb_ = chain_failure_cb

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
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def get(self, key):
        try:
            return self._parse_response(self.blocks[self.block_id(key)].get(key))
        except RedoError:
            return self._parse_response(self.blocks[self.block_id(key)].get(key))
        except RedirectError as e:
            return self._parse_response(BlockChainClient(self.client_cache, e.blocks).redirected_get(key))
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def exists(self, key):
        try:
            return self._parse_response(self.blocks[self.block_id(key)].exists(key)) == b'true'
        except RedoError:
            return self._parse_response(self.blocks[self.block_id(key)].exists(key)) == b'true'
        except RedirectError as e:
            return self._parse_response(BlockChainClient(self.client_cache, e.blocks).redirected_exists(key)) == b'true'
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def update(self, key, value):
        try:
            return self._parse_response(self.blocks[self.block_id(key)].update(key, value))
        except RedoError:
            return self._parse_response(self.blocks[self.block_id(key)].update(key, value))
        except RedirectError as e:
            return self._parse_response(BlockChainClient(self.client_cache, e.blocks).redirected_update(key, value))
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def remove(self, key):
        try:
            return self._parse_response(self.blocks[self.block_id(key)].remove(key))
        except RedoError:
            return self._parse_response(self.blocks[self.block_id(key)].remove(key))
        except RedirectError as e:
            return self._parse_response(BlockChainClient(self.client_cache, e.blocks).redirected_remove(key))
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def pipeline_get(self):
        return PipelinedGet(self.path, self.blocks, self.slots, self.chain_failure_cb_)

    def pipeline_put(self):
        return PipelinedPut(self.path, self.blocks, self.slots, self.chain_failure_cb_)

    def pipeline_update(self):
        return PipelinedUpdate(self.path, self.blocks, self.slots, self.chain_failure_cb_)

    def pipeline_remove(self):
        return PipelinedRemove(self.path, self.blocks, self.slots, self.chain_failure_cb_)

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
