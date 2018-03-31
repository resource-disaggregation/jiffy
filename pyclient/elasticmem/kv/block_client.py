import threading

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket

import block_request_service
import block_response_service


class PendingCallbacks:
    def __init__(self):
        self.condition = threading.Condition()
        self.callbacks = {}

    def put(self, client_id, callback):
        with self.condition:
            self.callbacks[client_id] = callback

    def get(self, client_id):
        with self.condition:
            callback = self.callbacks[client_id]
        return callback

    def remove(self, client_id):
        with self.condition:
            callback = self.callbacks[client_id]
            del self.callbacks[client_id]
            if not self.callbacks:
                self.condition.notify_all()
        return callback

    def wait(self):
        with self.condition:
            while self.callbacks:
                self.condition.wait()


class CommandResponseHandler(block_response_service.Iface):
    def __init__(self, callbacks):
        self.callbacks = callbacks

    def response(self, seq, result):
        cb = self.callbacks.remove(seq.client_seq_no)
        # del self.callbacks[seq.client_seq_no]
        # self.callbacks.remove(seq.client_seq_no)
        cb(result)


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
        self.protocol_ = TBinaryProtocol(self.transport_)
        self.client_ = block_request_service.Client(self.protocol_)
        self.transport_.open()

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        if self.transport_.isOpen():
            self.transport_.close()

    def add_response_listener(self, client_id, events):
        self.client_.register_client_id(self.id_, client_id)
        handler = CommandResponseHandler(events)
        processor = block_response_service.Processor(handler)
        return ResponseProcessor(processor, self.protocol_)

    def get_client_id(self):
        return self.client_.get_client_id()

    def send_request(self, seq, cmd_id, arguments):
        self.client_.command_request(seq, self.id_, cmd_id, arguments)


class ResultFuture:
    def __init__(self):
        self.condition = threading.Condition()
        self.result = None

    def __call__(self, result):
        with self.condition:
            self.result = result
            self.condition.notify_all()

    def get(self, timeout):
        with self.condition:
            while self.result is None:
                self.condition.wait(timeout)
            ret = self.result
        return ret


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
        self.callbacks = PendingCallbacks()
        self.response_processor = self.tail.add_response_listener(self.seq.client_id, self.callbacks)
        self.response_processor.start()

    def __del__(self):
        self.head.disconnect()
        self.tail.disconnect()
        self.response_processor.stop()

    def run_command_async(self, client, cmd_id, args, callback):
        op_seq = self.seq.client_seq_no
        self.callbacks.put(op_seq, callback)
        # self.events[op_seq] = callback
        client.send_request(self.seq, cmd_id, args)
        self.seq.client_seq_no += 1

    def run_command(self, client, cmd_id, args):
        future = ResultFuture()
        self.run_command_async(client, cmd_id, args, future)
        return future.get(self.request_timeout_s)

    def get_async(self, key, callback):
        self.run_command_async(self.tail, 0, [key], callback)

    def get(self, key):
        return self.run_command(self.tail, 0, [key])[0]

    def put_async(self, key, value, callback):
        self.run_command_async(self.tail, 1, [key, value], callback)

    def put(self, key, value):
        return self.run_command(self.tail, 1, [key, value])[0]

    def remove_async(self, key, callback):
        self.run_command_async(self.tail, 2, [key], callback)

    def remove(self, key):
        return self.run_command(self.tail, 2, [key])[0]

    def update_async(self, key, value, callback):
        self.run_command_async(self.tail, 3, [key, value], callback)

    def update(self, key, value):
        return self.run_command(self.tail, 3, [key, value])[0]

    def wait(self):
        self.callbacks.wait()
