from jiffy.storage.compat import b
from jiffy.storage.command import CommandType


class HashTableOps:
    exists = b('exists')
    get = b('get')
    put = b('put')
    remove = b('remove')
    update = b('update')
    upsert = b('upsert')

    op_types = {exists: CommandType.accessor,
                get: CommandType.accessor,
                put: CommandType.mutator,
                remove: CommandType.mutator,
                update: CommandType.mutator,
                upsert: CommandType.mutator}
