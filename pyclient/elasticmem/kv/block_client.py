import threading
from thrift.Thrift import TApplicationException, TMessageType
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated
from thrift.transport import TTransport, TSocket

import block_request_service
import block_response_service


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


class ResponseProcessor(threading.Thread):
    def __init__(self, processor, protocol):
        super(ResponseProcessor, self).__init__()
        self.processor = processor
        self.protocol = protocol
        self._stop_event = threading.Event()
        self.daemon = True

    def run(self):
        try:
            while not self.stopped():
                self.processor.process(self.protocol, self.protocol)
        except TTransport.TTransportException:
            pass
        except Exception:
            raise

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


class BlockClient:
    def __init__(self, host, port, block_id):
        self.id_ = block_id
        self.socket_ = TSocket.TSocket(host, port)
        self.transport_ = TTransport.TBufferedTransport(self.socket_)
        self.protocol_ = TBinaryProtocolAccelerated(self.transport_)
        self.client_ = block_request_service.Client(self.protocol_)
        self.transport_.open()

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        if self.transport_.isOpen():
            self.transport_.close()

    def get_response_reader(self, client_id):
        self.client_.register_client_id(self.id_, client_id)
        return CommandResponseReader(self.protocol_)

    def get_client_id(self):
        return self.client_.get_client_id()

    def send_request(self, seq, cmd_id, arguments):
        self.client_.command_request(seq, self.id_, cmd_id, arguments)


class BlockChainClient:
    def __init__(self, chain, request_timeout_s=3.0):
        self.seq = block_request_service.sequence_id(-1, 0, -1)
        self.chain = chain
        self.request_timeout_s = request_timeout_s
        h_host, h_port, _, _, _, h_bid = chain[0].split(':')
        self.head = BlockClient(h_host, int(h_port), int(h_bid))
        self.seq.client_id = self.head.get_client_id()
        if len(chain) == 1:
            self.tail = self.head
        else:
            t_host, t_port, _, _, _, t_bid = chain[-1].split(':')
            self.tail = BlockClient(t_host, int(t_port), int(t_bid))
        self.response_reader = self.tail.get_response_reader(self.seq.client_id)
        self.response_cache = {}

    def __del__(self):
        self.head.disconnect()
        self.tail.disconnect()

    def _send_cmd(self, client, cmd_id, args):
        op_seq = self.seq.client_seq_no
        client.send_request(self.seq, cmd_id, args)
        self.seq.client_seq_no += 1
        return op_seq

    def _recv_cmd(self, op_seq):
        if op_seq in self.response_cache:
            result = self.response_cache[op_seq]
            del self.response_cache[op_seq]
            return result

        while True:
            recv_seq, result = self.response_reader.recv_response()
            if op_seq == recv_seq:
                return result
            else:
                self.response_cache[recv_seq] = result

    def _run_command(self, client, cmd_id, args):
        seq = self._send_cmd(client, cmd_id, args)
        return self._recv_cmd(seq)

    def get(self, key):
        return self._run_command(self.tail, 0, [key])[0]

    def send_get(self, key):
        return self._send_cmd(self.tail, 0, [key])

    def put(self, key, value):
        return self._run_command(self.head, 1, [key, value])[0]

    def send_put(self, key, value):
        return self._send_cmd(self.head, 1, [key, value])

    def remove(self, key):
        return self._run_command(self.head, 2, [key])[0]

    def send_remove(self, key):
        return self._send_cmd(self.head, 2, [key])

    def update(self, key, value):
        return self._run_command(self.head, 3, [key, value])[0]

    def send_update(self, key, value):
        return self._send_cmd(self.head, 3, [key, value])

    def recv_response(self, op_seq):
        return self._recv_cmd(op_seq)[0]

    def recv_responses(self, op_seqs):
        return [self._recv_cmd(op_seq) for op_seq in op_seqs]