from jiffy.storage.replica_chain_client import ReplicaChainClient
from jiffy.storage.command import CommandType
from jiffy.storage.compat import b
from jiffy.storage.data_structure_client import DataStructureClient


class FileOps:
    write = b('write')
    read = b('read')
    seek = b('seek')

    op_types = {read: CommandType.accessor,
                seek: CommandType.accessor,
                write: CommandType.mutator}


class FileClient(DataStructureClient):
    def __init__(self, fs, path, block_info):
        super(FileClient, self).__init__(fs, path, block_info, FileOps.op_types)
        self.cur_partition = 0
        self.cur_offset = 0
        self.last_partition = 0

    def _handle_redirect(self, args, response):
        return response

    def _block_id(self, args):
        return self.cur_partition

    def _need_chain(self):
        return self.cur_partition >= len(self.blocks) - 1

    def _update_last_partition(self, partition):
        if self.last_partition < partition:
            self.last_partition = partition

    def seek(self, offset):
        response = self._run_repeated([FileOps.seek])
        size = int(response[1])
        cap = int(response[2])
        if offset > self.cur_partition * cap + size:
            return False
        else:
            self.cur_partition = int(offset / cap)
            self.cur_offset = int(offset % cap)
            return True


class FileReader(FileClient):
    def __init__(self, fs, path, block_info):
        super(FileReader, self).__init__(fs, path, block_info)

    def _handle_redirect(self, args, response):
        if response[0] == b('!ok'):
            self.cur_offset += len(response[1])
        elif response[0] == b('!redo'):
            return None
        elif response[0] == '!split_read':
            result = b('')
            while response[0] == b('!split_read'):
                data_part = response[1]
                result += data_part
                if self._need_chain():
                    chain = response[2].split('!')
                    self.blocks.append(
                        ReplicaChainClient(self.fs, self.path, self.client_cache, chain, FileOps.op_types))
                self.cur_partition += 1
                self._update_last_partition(self.cur_partition)
                self.cur_offset = 0

                new_args = [FileOps.read, b(str(self.cur_offset)), b(str(int(args[2]) - len(result)))]
                response = self.blocks[self.cur_partition].run_command(new_args)

                if response[0] == b('!ok'):
                    self.cur_offset += len(response[1])
                    result += response[1]
            response[1] = result
        return response

    def read(self, size):
        return self._run_repeated([FileOps.read, b(str(self.cur_offset)), b(str(size))])[1]


class FileWriter(FileClient):
    def __init__(self, fs, path, block_info):
        super(FileWriter, self).__init__(fs, path, block_info)

    def _handle_redirect(self, args, response):
        data = args[1]
        if response[0] == b('!ok'):
            self.cur_offset += len(data)
        elif response[0] == b('!redo'):
            return None
        elif response[0] == b('split_write'):
            while response[0] == b('!split_write'):
                remaining_data_len = int(response[1])
                remaining_data = data[len(data) - remaining_data_len: len(data)]

                if self._need_chain():
                    chain = response[2].split('!')
                    self.blocks.append(
                        ReplicaChainClient(self.fs, self.path, self.client_cache, chain, FileOps.op_types))

                self.cur_partition += 1
                self._update_last_partition(self.cur_partition)
                self.cur_offset = 0

                while True:
                    new_args = [FileOps.write, remaining_data, b(str(self.cur_offset))]
                    response = self.blocks[self.cur_partition].run_command(new_args)
                    if response[0] != b('!redo'):
                        break
                self.cur_offset += len(remaining_data)
        return response

    def write(self, data):
        self._run_repeated([FileOps.write, data, b(str(self.cur_offset))])
