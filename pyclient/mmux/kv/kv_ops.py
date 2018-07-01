class KVOpType:
    accessor = 0
    mutator = 1


class KVOps:
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
    op_types = [KVOpType.accessor,  # exists
                KVOpType.accessor,  # get
                KVOpType.accessor,  # keys
                KVOpType.accessor,  # num_keys
                KVOpType.mutator,  # put
                KVOpType.mutator,  # remove
                KVOpType.mutator,  # update
                KVOpType.mutator,  # lock
                KVOpType.mutator,  # unlock
                KVOpType.accessor,  # locked_data_in_slot_range
                KVOpType.accessor,  # locked_get
                KVOpType.accessor,  # locked_put
                KVOpType.accessor,  # locked_remove
                KVOpType.accessor]  # locked_update


def op_type(op):
    return KVOps.op_types[op]
