from jiffy.storage.crc import SLOT_MAX


class NameFormatter(object):
    def __init__(self):
        pass

    def get(self, i):
        pass


class DefaultNameFormatter(NameFormatter):
    def __init__(self):
        super(DefaultNameFormatter, self).__init__()

    def get(self, i):
        return str(i)


class HashTableNameFormatter(NameFormatter):
    def __init__(self, num_blocks):
        super(HashTableNameFormatter, self).__init__()
        self.num_blocks = num_blocks
        self.slot_range = int(SLOT_MAX / num_blocks)

    def get(self, i):
        return '{}_{}'.format(i * self.slot_range, SLOT_MAX if i == self.num_blocks - 1 else (i + 1) * self.slot_range)
