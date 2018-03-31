import logging

import block_client


class KVClient:
    def __init__(self, path, data_status, chain_failure_cb, hash_fn=hash):
        self.path = path
        self.file_info = data_status
        self.blocks = [block_client.BlockChainClient(chain.block_names) for chain in data_status.data_blocks]
        self.chain_failure_cb_ = chain_failure_cb
        self.hash_fn_ = hash_fn

    def put_async(self, key, value, callback):
        self.blocks[self.block_id(key)].put_async(key, value, callback)

    def put(self, key, value):
        try:
            return self.blocks[self.block_id(key)].put(key, value)
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def get_async(self, key, callback):
        self.blocks[self.block_id(key)].get_async(key, callback)

    def get(self, key):
        try:
            return self.blocks[self.block_id(key)].get(key)
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def update_async(self, key, value, callback):
        self.blocks[self.block_id(key)].update_async(key, value, callback)

    def update(self, key, value):
        try:
            return self.blocks[self.block_id(key)].update(key, value)
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def remove_async(self, key, callback):
        self.blocks[self.block_id(key)].remove_async(key, callback)

    def remove(self, key):
        try:
            return self.blocks[self.block_id(key)].remove(key)
        except RuntimeError as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def block_id(self, key):
        return self.hash_fn_(key) % len(self.blocks)

    def wait(self):
        for b in self.blocks:
            b.wait()

