from jiffy.storage.command import CommandType
from jiffy.storage.compat import b


class QueueOps:
    enqueue = b('enqueue')
    dequeue = b('dequeue')
    read_next = b('read_next')

    op_types = {enqueue: CommandType.mutator,
                dequeue: CommandType.mutator,
                read_next: CommandType.accessor}
