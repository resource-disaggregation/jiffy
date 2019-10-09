from jiffy.storage.command import CommandType
from jiffy.storage.compat import b
from jiffy.storage.data_structure_client import DataStructureClient
from jiffy.storage.replica_chain_client import ReplicaChainClient


class QueueOps:
    enqueue = b('enqueue')
    dequeue = b('dequeue')
    read_next = b('read_next')

    op_types = {enqueue: CommandType.mutator,
                dequeue: CommandType.mutator,
                read_next: CommandType.accessor}


class Queue(DataStructureClient):
    METADATA_LEN = 8

    def __init__(self, fs, path, block_info, timeout_ms=1000):
        super(Queue, self).__init__(fs, path, block_info, QueueOps.op_types, timeout_ms)
        self.enqueue_partition = 0
        self.dequeue_partition = 0
        self.read_partition = 0
        self.read_offset = 0

    def _handle_redirect(self, args, response):
        cmd = args[0]
        if response[0] == b('!ok'):
            if cmd == QueueOps.read_next:
                self.read_offset += self.METADATA_LEN + len(response[1])
            return response
        elif response[0] == b('!redo'):
            return None
        elif response[0] == b('!split_enqueue'):
            while response[0] == b('!split_enqueue'):
                remaining_data_len = int(response[1])
                data = args[1]
                remaining_data = data[len(data) - remaining_data_len: len(data)]

                if self.enqueue_partition >= len(self.blocks) - 1:
                    chain = response[2].split('!')
                    self.blocks.append(
                        ReplicaChainClient(self.fs, self.path, self.client_cache, chain, QueueOps.op_types))

                self.enqueue_partition += 1

                while True:
                    response = self.blocks[self.enqueue_partition].run_command([QueueOps.enqueue, remaining_data])
                    if response[0] != b('!redo'):
                        break
        elif response[0] == b('!split_dequeue'):
            result = b('')
            while response[0] == b('!split_dequeue'):
                data_part = response[1]
                result += data_part
                if self.dequeue_partition >= len(self.blocks) - 1:
                    chain = response[2].split('!')
                    self.blocks.append(
                        ReplicaChainClient(self.fs, self.path, self.client_cache, chain, QueueOps.op_types))
                self.dequeue_partition += 1
                response = self.blocks[self.dequeue_partition].run_command([QueueOps.dequeue])
                if response[0] == b('!ok'):
                    result += response[1]
            response[1] = result
        elif response[0] == b('!split_readnext'):
            result = b('')
            while response[0] == b('!split_dequeue'):
                data_part = response[1]
                result += data_part
                if self.read_partition >= len(self.blocks) - 1:
                    chain = response[2].split('!')
                    self.blocks.append(
                        ReplicaChainClient(self.fs, self.path, self.client_cache, chain, QueueOps.op_types))
                self.read_partition += 1
                self.read_offset = 0
                response = self.blocks[self.read_partition].run_command([QueueOps.read_next, str(self.read_partition)])
                if response[0] == b('!ok'):
                    self.read_offset += (self.METADATA_LEN + len(response[1]))
                    result += response[1]
            response[1] = result
        return response

    def _block_id(self, args):
        if args[0] == QueueOps.enqueue:
            return self.enqueue_partition
        elif args[0] == QueueOps.dequeue:
            return self.dequeue_partition
        elif args[0] == QueueOps.read_next:
            return self.read_partition
        raise ValueError

    def put(self, item):
        self._run_repeated([QueueOps.enqueue, item])

    def get(self):
        return self._run_repeated([QueueOps.dequeue])[1]

    def read_next(self):
        return self._run_repeated([QueueOps.read_next])[1]

    class ReadIterator(object):
        def __init__(self, q):
            self.q = q

        def __iter__(self):
            return self

        def __next__(self):
            return self.q.read_next()

    def read_iterator(self):
        return Queue.ReadIterator(self)
