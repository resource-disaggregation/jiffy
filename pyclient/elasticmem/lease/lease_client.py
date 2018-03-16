from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket

import directory_lease_service


class LeaseAck:
    def __init__(self, renewed, flushed, removed):
        self.renewed = renewed
        self.flushed = flushed
        self.removed = removed


class LeaseClient:
    def __init__(self, host, port):
        self.socket_ = TSocket.TSocket(host, port)
        self.transport_ = TTransport.TBufferedTransport(self.socket_)
        self.protocol_ = TBinaryProtocol(self.transport_)
        self.client_ = directory_lease_service.Client(self.protocol_)
        self.transport_.open()

    def __del__(self):
        self.close()

    def close(self):
        if self.transport_.isOpen():
            self.transport_.close()

    def update_lease(self, to_renew, to_flush, to_remove):
        ack = self.client_.update_leases(directory_lease_service.rpc_lease_update(to_renew, to_flush, to_remove))
        return LeaseAck(ack.renewed, ack.flushed, ack.removed)
