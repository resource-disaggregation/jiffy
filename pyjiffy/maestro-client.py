from maestro_allocator_service import maestro_allocator_service

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:
    print("starting thrift client...")
    transport = TSocket.TSocket('127.0.0.1', 9090)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = maestro_allocator_service.Client(protocol)
    transport.open()

    print(client.allocate("POMNQW", 5))
    print(client.deallocate("ASDASD", ["172.17.0.3:9095:9093:1", "172.17.0.3:9095:9093:10"]))


    transport.close()

except Thrift.TException as tx:
    print(str(tx))
