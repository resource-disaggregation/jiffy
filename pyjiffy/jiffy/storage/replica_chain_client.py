import logging
import socket

from thrift.transport.TTransport import TTransportException

from jiffy.directory.directory_client import ReplicaChain
from jiffy.directory.ttypes import rpc_replica_chain
from jiffy.storage import block_request_service
from jiffy.storage.block_client import BlockClient
from jiffy.storage.compat import b, bytes_to_str
from jiffy.storage.hash_table_ops import CommandType, op_type, HashTableOps


class ReplicaChainClient:
    class LockedClient:
        def __init__(self, parent):
            self.parent = parent
            response = b(self.run_command(HashTableOps.lock, [])[0])
            if response != b("!ok"):
                self.redirecting = True
                self.redirect_chain = [bytes_to_str(x) for x in response[1:].split(b('!'))]
            else:
                self.redirecting = False
                self.redirect_chain = []

        def __del__(self):
            if self.parent.is_open():
                self.unlock()

        def unlock(self):
            self.run_command(HashTableOps.unlock, [])

        def get_chain(self):
            return self.parent.get_chain()

        def is_redirecting(self):
            return self.redirecting

        def get_redirect_chain(self):
            return self.redirect_chain

        def send_command(self, cmd_id, args):
            self.parent.send_command(cmd_id, args)

        def recv_response(self):
            return self.parent.recv_response()

        def run_command(self, cmd_id, args):
            return self.parent.run_command(cmd_id, args)

        def run_command_redirected(self, cmd_id, args):
            return self.parent.run_command_redirected(cmd_id, args)

    def __init__(self, fs, path, client_cache, chain):
        self.fs = fs
        self.path = path
        self.client_cache = client_cache
        self.chain = chain
        self._init()

    def _init(self):
        self.seq = block_request_service.sequence_id(-1, 0, -1)
        h_host, h_port, _, _, _, h_bid = self.chain.block_names[0].split(':')
        self.head = BlockClient(self.client_cache, h_host, int(h_port), int(h_bid))
        self.seq.client_id = self.head.get_client_id()
        if len(self.chain.block_names) == 1:
            self.tail = self.head
        else:
            t_host, t_port, _, _, _, t_bid = self.chain.block_names[-1].split(':')
            self.tail = BlockClient(self.client_cache, t_host, int(t_port), int(t_bid))
        self.response_reader = self.tail.get_response_reader(self.seq.client_id)
        self.response_cache = {}
        self.in_flight = False

    def _invalidate_cache(self):
        for block in self.chain.block_names:
            host, port, _, _, _, _ = block.split(':')
            self.client_cache.remove(host, int(port))

    def lock(self):
        return self.LockedClient(self)

    def get_chain(self):
        return self.chain

    def is_open(self):
        return self.head.is_open() and self.tail.is_open()

    def _send_command(self, client, cmd_id, args):
        if self.in_flight:
            raise RuntimeError("Cannot have more than one request in-flight")
        client.send_request(self.seq, cmd_id, args)
        self.in_flight = True

    def send_command(self, cmd_id, args):
        if op_type(cmd_id) == CommandType.accessor:
            self._send_command(self.tail, cmd_id, args)
        else:
            self._send_command(self.head, cmd_id, args)

    def _recv_response(self):
        rseq, result = self.response_reader.recv_response()
        if self.seq.client_seq_no != rseq:
            raise RuntimeError("SEQ: Expected={} Received={}".format(self.seq.client_seq_no, rseq))
        self.seq.client_seq_no += 1
        self.in_flight = False
        return result

    def recv_response(self):
        return self._recv_response()

    def _run_command(self, client, cmd_id, args):
        self._send_command(client, cmd_id, args)
        return self._recv_response()

    def run_command(self, cmd_id, args):
        resp = None
        retry = False
        while resp is None:
            try:
                if op_type(cmd_id) == CommandType.accessor:
                    resp = self._run_command(self.tail, cmd_id, args)
                else:
                    resp = self._run_command(self.head, cmd_id, args)
                    if retry and resp[0] == b('!duplicate_key'):
                        resp[0] = b('!ok')
            except (TTransportException, socket.timeout) as e:
                logging.warning("Error in connection to chain {}: {}".format(self.chain.block_names, e))
                rchain = self.fs.resolve_failures(self.path, rpc_replica_chain(self.chain.block_names,
                                                                               self.chain.name,
                                                                               self.chain.metadata,
                                                                               self.chain.storage_mode))
                self.chain = ReplicaChain(rchain.block_names, rchain.name, rchain.metadata, rchain.storage_mode)
                logging.warning("Updated chain: {}".format(self.chain.block_names))
                # invalidate the client cache for the failed connection(s)
                self._invalidate_cache()
                self._init()
                retry = True
        return resp

    def _run_command_redirected(self, client, cmd_id, args):
        args.append("!redirected")
        self._send_command(client, cmd_id, args)
        return self._recv_response()

    def run_command_redirected(self, cmd_id, args):
        if op_type(cmd_id) == CommandType.accessor:
            return self._run_command_redirected(self.tail, cmd_id, args)
        else:
            return self._run_command_redirected(self.head, cmd_id, args)
