from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket

import block_service


class BlockConnection:
    def __init__(self, block_name):
        host, port, block_id = block_name.split(':')
        self.id_ = int(block_id)
        self.socket_ = TSocket.TSocket(host, int(port))
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
        self.client_.run_command(self.id_, 1, [key, value])

    def get(self, key):
        return self.client_.run_command(self.id_, 0, [key])[0]

    def update(self, key, value):
        self.client_.run_command(self.id_, 3, [key, value])

    def remove(self, key):
        self.client_.run_command(self.id_, 2, [key])


class KVClient:
    def __init__(self, block_names, hash_fn=hash):
        self.connections_ = [BlockConnection(block_name) for block_name in block_names]
        self.hash_fn_ = hash_fn

    def __del__(self):
        self.close()

    def close(self):
        for x in self.connections_:
            x.close()

    def put(self, key, value):
        self.connections_[self.block_id(key)].put(key, value)

    def get(self, key):
        return self.connections_[self.block_id(key)].get(key)

    def update(self, key, value):
        self.connections_[self.block_id(key)].update(key, value)

    def remove(self, key):
        self.connections_[self.block_id(key)].remove(key)

    def block_id(self, key):
        return self.hash_fn_(key) % len(self.connections_)
