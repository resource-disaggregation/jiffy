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
    op_types = [KVOpType.accessor, KVOpType.accessor, KVOpType.accessor, KVOpType.accessor, KVOpType.mutator,
                KVOpType.mutator, KVOpType.mutator]


def op_type(op):
    return KVOps.op_types[op]
