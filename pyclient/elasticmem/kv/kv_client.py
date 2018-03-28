import logging

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket

import block_service


class BlockConnection:
    def __init__(self, block_name):
        host, service_port, management_port, notification_port, block_id = block_name.split(':')
        self.id_ = int(block_id)
        self.socket_ = TSocket.TSocket(host, int(service_port))
        self.transport_ = TTransport.TBufferedTransport(self.socket_)
        self.protocol_ = TBinaryProtocol(self.transport_)
        self.client_ = block_service.Client(self.protocol_)
        self.transport_.open()

    def __del__(self):
        self.close()

    def close(self):
        if self.transport_.isOpen():
            self.transport_.close()

    def put(self, key, value):
        self.client_.run_command(-1, self.id_, 1, [key, value])

    def get(self, key):
        return self.client_.run_command(-1, self.id_, 0, [key])[0]

    def update(self, key, value):
        self.client_.run_command(-1, self.id_, 3, [key, value])

    def remove(self, key):
        self.client_.run_command(-1, self.id_, 2, [key])


class KVClient:
    def __init__(self, path, data_status, chain_failure_cb, hash_fn=hash):
        self.path = path
        self.file_info = data_status
        self.singleton = data_status.chain_length == 1
        if self.singleton:
            self.heads_ = [BlockConnection(block_chain.block_names[0]) for block_chain in data_status.data_blocks]
            self.tails_ = self.heads_
        else:
            self.heads_ = [BlockConnection(block_chain.block_names[0]) for block_chain in data_status.data_blocks]
            self.tails_ = [BlockConnection(block_chain.block_names[-1]) for block_chain in data_status.data_blocks]
        self.chain_failure_cb_ = chain_failure_cb
        self.hash_fn_ = hash_fn

    def __del__(self):
        self.close()

    def close(self):
        for x in self.heads_:
            x.close()
        if not self.singleton:
            for x in self.tails_:
                x.close()

    def put(self, key, value):
        try:
            self.heads_[self.block_id(key)].put(key, value)
        except block_service.chain_failure_exception as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def get(self, key):
        try:
            return self.tails_[self.block_id(key)].get(key)
        except block_service.chain_failure_exception as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def update(self, key, value):
        try:
            self.heads_[self.block_id(key)].update(key, value)
        except block_service.chain_failure_exception as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def remove(self, key):
        try:
            self.heads_[self.block_id(key)].remove(key)
        except block_service.chain_failure_exception as e:
            logging.warning(e)
            self.chain_failure_cb_(self.path, self.file_info.data_blocks[self.block_id(key)])

    def block_id(self, key):
        return self.hash_fn_(key) % len(self.heads_)
