from jiffy.storage.command import CommandType


class HashTableOps:
    exists = 0
    get = 1
    keys = 2
    num_keys = 3
    put = 4
    remove = 5
    update = 6
    lock = 7
    unlock = 8
    locked_data_in_slot_range = 9
    locked_get = 10
    locked_put = 11
    locked_remove = 12
    locked_update = 13
    op_types = [CommandType.accessor,  # exists
                CommandType.accessor,  # get
                CommandType.accessor,  # keys
                CommandType.accessor,  # num_keys
                CommandType.mutator,  # put
                CommandType.mutator,  # remove
                CommandType.mutator,  # update
                CommandType.mutator,  # lock
                CommandType.mutator,  # unlock
                CommandType.accessor,  # locked_data_in_slot_range
                CommandType.accessor,  # locked_get
                CommandType.accessor,  # locked_put
                CommandType.accessor,  # locked_remove
                CommandType.accessor]  # locked_update


def op_type(op):
    return HashTableOps.op_types[op]
