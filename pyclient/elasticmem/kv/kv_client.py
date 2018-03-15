from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket

import kv_rpc_service


class BlockConnection:
    def __init__(self, block_name):
        host, port, block_id = block_name.split(':')
        print "BlockConnection: host=%s, port=%d, block_id=%d" % (host, int(port), int(block_id))
        self.id_ = int(block_id)
        self.socket_ = TSocket.TSocket(host, int(port))
        self.transport_ = TTransport.TBufferedTransport(self.socket_)
        self.protocol_ = TBinaryProtocol(self.transport_)
        self.client_ = kv_rpc_service.Client(self.protocol_)
        self.transport_.open()

    def __del__(self):
        self.close()

    def close(self):
        if self.transport_.isOpen():
            print "Closing BlockConnection"
            self.transport_.close()

    def put(self, key, value):
        self.client_.put(self.id_, key, value)

    def get(self, key):
        return self.client_.get(self.id_, key)

    def update(self, key, value):
        self.client_.update(self.id_, key, value)

    def remove(self, key):
        self.client_.remove(self.id_, key)


class KVClient:
    def __init__(self, block_names, hash_fn=hash):
        print block_names
        self.connections_ = [BlockConnection(block_name) for block_name in block_names]
        self.hash_fn_ = hash_fn
        self.bytes_written = 0

    def __del__(self):
        self.close()

    def close(self):
        for x in self.connections_:
            x.close()

    def put(self, key, value):
        self.bytes_written += len(key) + len(value)
        self.connections_[self.block_id(key)].put(key, value)

    def get(self, key):
        return self.connections_[self.block_id(key)].get(key)

    def update(self, key, value):
        self.bytes_written += len(key) + len(value)
        self.connections_[self.block_id(key)].update(key, value)

    def remove(self, key):
        self.bytes_written -= len(key)  # TODO: somehow get value size too
        self.connections_[self.block_id(key)].remove(key)

    def block_id(self, key):
        return self.hash_fn_(key) % len(self.connections_)
