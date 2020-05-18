from maestro_allocator_service import maestro_allocator_service
from jiffy.client import JiffyClient
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:
    # print("starting thrift client...")
    # transport = TSocket.TSocket('127.0.0.1', 9090)
    # transport = TTransport.TBufferedTransport(transport)
    # protocol = TBinaryProtocol.TBinaryProtocol(transport)
    # client = maestro_allocator_service.Client(protocol)
    # transport.open()

    # print(client.allocate("POMNQW", 5))
    # print(client.deallocate("ASDASD", ["172.17.0.3:9095:9093:1", "172.17.0.3:9095:9093:10"]))
    # transport.close()

    client = JiffyClient(host="127.0.0.1", directory_service_port=9001, lease_port=9091, timeout_ms=10000)

    in_file = open("setup.cfg", "rb").read()

    fileclient = client.create_file("/abh", "")
    fileclient.write(in_file)
    fileInBytes = fileclient.read(1000000)

    fileclient = client.open_file("/abf")
    fileInBytes = fileclient.read(3)
    print(fileInBytes)

except Thrift.TException as tx:
    print(str(tx))
