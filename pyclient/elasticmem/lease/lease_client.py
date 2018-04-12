from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated
from thrift.transport import TTransport, TSocket

from elasticmem.lease import lease_service


class LeaseClient:
    def __init__(self, host, port):
        self.socket_ = TSocket.TSocket(host, port)
        self.transport_ = TTransport.TBufferedTransport(self.socket_)
        self.protocol_ = TBinaryProtocolAccelerated(self.transport_)
        self.client_ = lease_service.Client(self.protocol_)
        self.transport_.open()

    def __del__(self):
        self.close()

    def close(self):
        if self.transport_.isOpen():
            self.transport_.close()

    def renew_leases(self, to_renew):
        return self.client_.renew_leases(to_renew)
