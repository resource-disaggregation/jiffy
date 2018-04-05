import logging

import block_client


class KVClient:
    def __init__(self, path, data_status, chain_failure_cb, hash_fn=hash):
        self.path = path
        self.file_info = data_status
        self.blocks = [block_client.BlockChainClient(chain.block_names) for chain in data_status.data_blocks]
        self.chain_failure_cb_ = chain_failure_cb
        self.hash_fn_ = hash_fn

    def send_put(self, key, value):
        bid = self.block_id(key)
        return bid, self.blocks[bid].send_put(key, value)

    def put(self, key, value):
        try:
            return self.blocks[self.block_id(key)].put(key, value)
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def send_get(self, key):
        bid = self.block_id(key)
        return bid, self.blocks[bid].send_get(key)

    def get(self, key):
        try:
            return self.blocks[self.block_id(key)].get(key)
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def send_update(self, key, value):
        bid = self.block_id(key)
        return bid, self.blocks[bid].send_update(key, value)

    def update(self, key, value):
        try:
            return self.blocks[self.block_id(key)].update(key, value)
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

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

