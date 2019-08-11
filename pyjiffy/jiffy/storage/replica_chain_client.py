import logging
import socket

from thrift.transport.TTransport import TTransportException

from jiffy.directory.directory_client import ReplicaChain
from jiffy.directory.ttypes import rpc_replica_chain
from jiffy.storage import block_request_service
from jiffy.storage.block_client import BlockClient
from jiffy.storage.compat import b
from jiffy.storage.command import CommandType


class ReplicaChainClient:
    def __init__(self, fs, path, client_cache, chain, cmd_type):
        self.fs = fs
        self.path = path
        self.client_cache = client_cache
        self.chain = chain
        self.cmd_type = cmd_type
        self._init()

    def _init(self):
        self.seq = block_request_service.sequence_id(-1, 0, -1)
        h_host, h_port, _, h_bid = self.chain.block_ids[0].split(':')
        self.head = BlockClient(self.client_cache, h_host, int(h_port), int(h_bid))
        self.seq.client_id = self.head.get_client_id()
        if len(self.chain.block_ids) == 1:
            self.tail = self.head
        else:
            t_host, t_port, _, t_bid = self.chain.block_ids[-1].split(':')
            self.tail = BlockClient(self.client_cache, t_host, int(t_port), int(t_bid))
        self.response_reader = self.tail.get_response_reader(self.seq.client_id)
        self.response_cache = {}
        self.in_flight = False

    def _invalidate_cache(self):
        for block in self.chain.block_ids:
            host, port, _, _ = block.split(':')
            self.client_cache.remove(host, int(port))

    def get_chain(self):
        return self.chain

    def is_open(self):
        return self.head.is_open() and self.tail.is_open()

    def _send_command(self, client, args):
        if self.in_flight:
            raise RuntimeError("Cannot have more than one request in-flight")
        client.send_request(self.seq, args)
        self.in_flight = True

    def send_command(self, args):
        if self.cmd_type[args[0]] == CommandType.accessor:
            self._send_command(self.tail, args)
        else:
            self._send_command(self.head, args)

    def _recv_response(self):
        rseq, result = self.response_reader.recv_response()
        if self.seq.client_seq_no != rseq:
            raise EOFError("SEQ: Expected={} Received={}".format(self.seq.client_seq_no, rseq))
        self.seq.client_seq_no += 1
        self.in_flight = False
        return result

    def recv_response(self):
        return self._recv_response()

    def _run_command(self, client, args):
        self._send_command(client, args)
        return self._recv_response()

    def run_command(self, args):
        resp = None
        retry = False
        while resp is None:
            try:
                if self.cmd_type[args[0]] == CommandType.accessor:
                    resp = self._run_command(self.tail, args)
                else:
                    resp = self._run_command(self.head, args)
                    if retry and resp[0] == b('!duplicate_key'):
                        resp[0] = b('!ok')
            except (TTransportException, socket.timeout) as e:
                logging.warning("Error in connection to chain {}: {}".format(self.chain.block_ids, e))
                rchain = self.fs.resolve_failures(self.path, rpc_replica_chain(self.chain.block_ids,
                                                                               self.chain.name,
                                                                               self.chain.metadata,
                                                                               self.chain.storage_mode))
                self.chain = ReplicaChain(rchain.block_ids, rchain.name, rchain.metadata, rchain.storage_mode)
                logging.warning("Updated chain: {}".format(self.chain.block_ids))
                # invalidate the client cache for the failed connection(s)
                self._invalidate_cache()
                self._init()
                retry = True
            except EOFError:
                resp = [b('!block_moved')]
        return resp

    def _run_command_redirected(self, client, args):
        if args[-1] != b('!redirected'):
            args.append(b('!redirected'))
        self._send_command(client, args)
        return self._recv_response()

    def run_command_redirected(self, args):
        if self.cmd_type[args[0]] == CommandType.accessor:
            return self._run_command_redirected(self.tail, args)
        else:
            return self._run_command_redirected(self.head, args)
