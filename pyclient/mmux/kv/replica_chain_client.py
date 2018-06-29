from mmux.kv import block_request_service
from mmux.kv.block_client import BlockClient
from mmux.kv.compat import b, bytes_to_str
from mmux.kv.kv_ops import KVOpType, op_type, KVOps


class ReplicaChainClient:
    class LockedClient:
        def __init__(self, parent):
            self.parent = parent
            response = b(self.run_command(KVOps.lock, [])[0])
            if response != b("!ok"):
                self.redirecting = True
                self.redirect_chain = [bytes_to_str(x) for x in response[1:].split(b('!'))]
            else:
                self.redirecting = False
                self.redirect_chain = []

        def __del__(self):
            self.unlock()

        def unlock(self):
            self.run_command(KVOps.unlock, [])

        def get_chain(self):
            return self.parent.get_chain()

        def is_redirecting(self):
            return self.redirecting

        def get_redirect_chain(self):
            return self.redirect_chain

        def send_cmd(self, cmd_id, args):
            self.parent.send_cmd(cmd_id, args)

        def recv_cmd(self):
            return self.parent.recv_cmd()

        def run_command(self, cmd_id, args):
            return self.parent.run_command(cmd_id, args)

        def run_command_redirected(self, cmd_id, args):
            return self.parent.run_command_redirected(cmd_id, args)

    def __init__(self, client_cache, chain, request_timeout_s=3.0):
        self.seq = block_request_service.sequence_id(-1, 0, -1)
        self.chain = chain
        self.request_timeout_s = request_timeout_s
        h_host, h_port, _, _, _, h_bid = chain[0].split(':')
        self.head = BlockClient(client_cache, h_host, int(h_port), int(h_bid))
        self.seq.client_id = self.head.get_client_id()
        if len(chain) == 1:
            self.tail = self.head
        else:
            t_host, t_port, _, _, _, t_bid = chain[-1].split(':')
            self.tail = BlockClient(client_cache, t_host, int(t_port), int(t_bid))
        self.response_reader = self.tail.get_response_reader(self.seq.client_id)
        self.response_cache = {}
        self.in_flight = False

    def get_chain(self):
        return self.chain

    def _send_cmd(self, client, cmd_id, args):
        if self.in_flight:
            raise RuntimeError("Cannot have more than one request in-flight")
        client.send_request(self.seq, cmd_id, args)
        self.in_flight = True

    def send_cmd(self, cmd_id, args):
        if op_type(cmd_id) == KVOpType.accessor:
            self._send_cmd(self.tail, cmd_id, args)
        else:
            self._send_cmd(self.head, cmd_id, args)

    def _recv_cmd(self):
        rseq, result = self.response_reader.recv_response()
        if self.seq.client_seq_no != rseq:
            raise RuntimeError("SEQ: Expected={} Received={}".format(self.seq.client_seq_no, rseq))
        self.seq.client_seq_no += 1
        self.in_flight = False
        return result

    def recv_cmd(self):
        return self._recv_cmd()

    def _run_command(self, client, cmd_id, args):
        self._send_cmd(client, cmd_id, args)
        return self._recv_cmd()

    def run_command(self, cmd_id, args):
        if op_type(cmd_id) == KVOpType.accessor:
            return self._run_command(self.tail, cmd_id, args)
        else:
            return self._run_command(self.head, cmd_id, args)

    def _run_command_redirected(self, client, cmd_id, args):
        args.append("!redirected")
        self._send_cmd(client, cmd_id, args)
        return self._recv_cmd()

    def run_command_redirected(self, cmd_id, args):
        if op_type(cmd_id) == KVOpType.accessor:
            return self._run_command_redirected(self.tail, cmd_id, args)
        else:
            return self._run_command_redirected(self.head, cmd_id, args)
