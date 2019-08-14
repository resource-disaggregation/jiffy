from time import sleep

from jiffy.storage.command import CommandType
from jiffy.storage.compat import b
from jiffy.storage.data_structure_client import DataStructureClient


class QueueOps:
    enqueue = b('enqueue')
    dequeue = b('dequeue')
    read_next = b('read_next')

    op_types = {enqueue: CommandType.mutator,
                dequeue: CommandType.mutator,
                read_next: CommandType.accessor}


class Queue(DataStructureClient):
    def __init__(self, fs, path, block_info, timeout_ms=100000):
        super(Queue, self).__init__(fs, path, block_info, QueueOps.op_types, timeout_ms)

    def _handle_redirect(self, args, response):
        return response

    def _block_id(self, args):
        return 0

    def put(self, item):
        self._run_repeated([QueueOps.enqueue, item])

    def get(self):
        return self._run_repeated([QueueOps.dequeue])[1]


class BlockingQueue(Queue):
    def __init__(self, fs, path, block_info, timeout_ms=100000):
        super(BlockingQueue, self).__init__(fs, path, block_info, timeout_ms)
        self.wait_time = 0.001

    def _handle_redirect(self, args, response):
        if response[0] == b('!full'):
            sleep(self.wait_time)
            return None
        elif response[0] == b('!empty'):
            sleep(self.wait_time)
            return None
        return response
