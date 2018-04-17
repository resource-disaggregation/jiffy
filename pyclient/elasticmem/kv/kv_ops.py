class KVOpType:
    accessor = 0
    mutator = 1


class KVOps:
    get = 0
    num_keys = 1
    put = 2
    remove = 3
    update = 4
    op_types = [KVOpType.accessor, KVOpType.accessor, KVOpType.mutator, KVOpType.mutator, KVOpType.mutator]


def op_type(op):
    return KVOps.op_types[op]
