import logging

from thrift.Thrift import TApplicationException, TMessageType
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated
from thrift.transport import TTransport, TSocket
from thrift.transport.TTransport import TTransportException

from jiffy.storage import block_request_service
from jiffy.storage import block_response_service


class ClientEntry:
    def __init__(self, transport, protocol, client):
        self.transport = transport
        self.protocol = protocol
        self.client = client


class BlockClientCache:
    def __init__(self, timeout_ms):
        self.cache = {}
        self.timeout_ms = timeout_ms

    def __del__(self):
        for k in self.cache:
            client_entry = self.cache[k]
            if client_entry.transport.isOpen():
                client_entry.transport.close()

    def remove(self, host, port):
        if (host, port) in self.cache:
            if self.cache[(host, port)].transport.isOpen():
                self.cache[(host, port)].transport.close()
            del self.cache[(host, port)]

    def get(self, host, port):
        if (host, port) in self.cache:
            entry = self.cache[(host, port)]
            return entry.transport, entry.protocol, entry.client

        socket = TSocket.TSocket(host, port)
        socket.setTimeout(self.timeout_ms)
        transport = TTransport.TBufferedTransport(socket, 1024 * 1024)  # Increase buffer-size
        protocol = TBinaryProtocolAccelerated(transport)
        client = block_request_service.Client(protocol)

        ex = None
        for i in range(3):
            try:
                transport.open()
            except TTransportException as e:
                ex = e
                continue
            except Exception:
                raise
            else:
                break
        else:
            raise TTransportException(ex.type, "Connection failed {}:{}: {}".format(host, port, ex.message))
        self.cache[(host, port)] = ClientEntry(transport, protocol, client)
        return transport, protocol, client


class CommandResponseReader:
    def __init__(self, iprot):
        self.iprot = iprot

    def recv_response(self):
        (fname, mtype, rseqid) = self.iprot.readMessageBegin()
        if mtype == TMessageType.EXCEPTION:
            x = TApplicationException()
            x.read(self.iprot)
            self.iprot.readMessageEnd()
            raise x
        args = block_response_service.response_args()
        args.read(self.iprot)
        self.iprot.readMessageEnd()
        if args.seq is not None and args.result is not None:
            return args.seq.client_seq_no, args.result
        raise TApplicationException(TApplicationException.MISSING_RESULT, "Command response failure: unknown result")

    def recv_responses(self, count):
        return [self.recv_response() for _ in range(count)]


class BlockClient:
    def __init__(self, client_cache, host, port, block_id):
        self.transport_, self.protocol_, self.client_ = client_cache.get(host, port)
        self.id_ = block_id

    def is_open(self):
        return self.transport_.isOpen()

    def get_response_reader(self, client_id):
        self.client_.register_client_id(self.id_, client_id)
        return CommandResponseReader(self.protocol_)

    def get_client_id(self):
        return self.client_.get_client_id()

    def send_request(self, seq, cmd_id, arguments):
        self.client_.command_request(seq, self.id_, cmd_id, arguments)