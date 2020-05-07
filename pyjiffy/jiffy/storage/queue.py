from enum import IntEnum
import copy

from jiffy.storage.command import CommandType
from jiffy.storage.compat import b, unicode, bytes, long, basestring, bytes_to_str
from jiffy.storage.data_structure_client import DataStructureClient
from jiffy.storage.replica_chain_client import ReplicaChainClient
from jiffy.directory.directory_client import ReplicaChain
from jiffy.directory.ttypes import rpc_storage_mode

class FifoQueueSizeType(IntEnum):
    HEAD_SIZE = 0
    TAIL_SIZE = 1

class QueueOps:
    enqueue = b('enqueue')
    dequeue = b('dequeue')
    read_next = b('read_next')
    length = b('length')
    in_rate = b('in_rate')
    out_rate = b('out_rate')

    op_types = {enqueue: CommandType.mutator,
                dequeue: CommandType.mutator,
                read_next: CommandType.accessor,
                length: CommandType.accessor,
                in_rate: CommandType.accessor,
                out_rate: CommandType.accessor}


class Queue(DataStructureClient):
    METADATA_LEN = 8

    def __init__(self, fs, path, block_info, timeout_ms):
        super(Queue, self).__init__(fs, path, block_info, QueueOps.op_types, timeout_ms)
        self.enqueue_partition = 0
        self.dequeue_partition = 0
        self.read_partition = 0
        self.start = 0
        if self.block_info.tags.get("fifoqueue.auto_scale") is None:
            self.auto_scale = True
        else:
            self.auto_scale = self.block_info.tags.get("fifoqueue.auto_scale")

    def _refresh(self):
        self._refresh()
        self.start = int(self.block_info.data_blocks[0])
        self.enqueue_partition = len(self.block_info.data_blocks) - 1
        self.dequeue_partition = 0
        if self.read_partition < self.start:
            self.read_partition = self.start


    def _handle_redirect(self, args, response):
        cmd = args[0]
        if response[0] == b('!ok'):
            return response
        elif response[0] == b('!redo'):
            return None
        if response[0][:11] == b('!redirected'):
            redirected_response = response[0]
            while response[0] == redirected_response:
                self.add_blocks(response, args)
                self.handle_partition_id(args)
                while True:
                    args_copy = copy.deepcopy(args)
                    if args[0] == QueueOps.enqueue:
                        args_copy.extend(response[-3:])
                    elif args[0] == QueueOps.dequeue:
                        args_copy.extend(response[-2:])
                    response = self.blocks[self._block_id(args)].run_command_redirected(args_copy)
                    if response[0] != b('!redo'):
                        break
        if response[0] == b("!block_moved"):
            self._refresh()
            return None
        return response

    def _block_id(self, args):
        if args[0] == QueueOps.enqueue:
            return self.enqueue_partition
        elif args[0] == QueueOps.dequeue:
            return self.dequeue_partition
        elif args[0] == QueueOps.read_next:
            return self.read_partition - self.start
        elif args[0] == QueueOps.length:
            if int(args[1]) == FifoQueueSizeType.HEAD_SIZE:
                return self.enqueue_partition
            else:
                return self.dequeue_partition
        elif args[0] == QueueOps.in_rate:
            return self.enqueue_partition
        elif args[0] == QueueOps.out_rate:
            return self.dequeue_partition
        else:
            raise ValueError

    def put(self, item):
        self._run_repeated([QueueOps.enqueue, item])

    def get(self):
        return self._run_repeated([QueueOps.dequeue])[1]

    def read_next(self):
        return self._run_repeated([QueueOps.read_next])[1]

    def length(self):
        tail = int(self._run_repeated([QueueOps.length, b(str(int(FifoQueueSizeType.TAIL_SIZE)))])[1])
        head = int(self._run_repeated([QueueOps.length, b(str(int(FifoQueueSizeType.HEAD_SIZE)))])[1])
        return head - tail

    def in_rate(self):
        return float(bytes_to_str(self._run_repeated([QueueOps.in_rate])[1]))

    def out_rate(self):
        return float(bytes_to_str(self._run_repeated([QueueOps.out_rate])[1]))

    def handle_partition_id(self, args):
        if args[0] == QueueOps.enqueue or (args[0] == QueueOps.length and int(args[1]) == FifoQueueSizeType.HEAD_SIZE) or args[0] == QueueOps.in_rate:
            self.enqueue_partition += 1
        elif args[0] == QueueOps.dequeue or (args[0] == QueueOps.length and int(args[1]) == FifoQueueSizeType.TAIL_SIZE) or args[0] == QueueOps.out_rate:
            self.dequeue_partition += 1
            if self.dequeue_partition > self.enqueue_partition:
                self.enqueue_partition = self.dequeue_partition
            dequeue_partition_name = self.dequeue_partition + self.start
            if dequeue_partition_name > self.read_partition:
                self.read_partition = dequeue_partition_name
        elif args[0] == QueueOps.read_next:
            self.read_partition += 1
            if self.read_partition - self.start > self.enqueue_partition:
                self.enqueue_partition = self.read_partition - self.start
        else:
            raise ValueError

    def add_blocks(self, response, args):
        if self._block_id(args) >= len(self.blocks) - 1:
            if self.auto_scale:
                block_ids = [bytes_to_str(j) for j in response[1].split(b('!'))]
                chain = ReplicaChain(block_ids, 0, 0, rpc_storage_mode.rpc_in_memory)
                self.blocks.append(
                    ReplicaChainClient(self.fs, self.path, self.client_cache, chain, QueueOps.op_types))
            else:
                raise ValueError


    class ReadIterator(object):
        def __init__(self, q):
            self.q = q

        def __iter__(self):
            return self

        def __next__(self):
            return self.q.read_next()

    def read_iterator(self):
        return Queue.ReadIterator(self)



