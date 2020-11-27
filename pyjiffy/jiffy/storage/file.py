from jiffy.storage.replica_chain_client import ReplicaChainClient
from jiffy.storage.command import CommandType
from jiffy.storage.compat import b, unicode, bytes, long, basestring, bytes_to_str
from jiffy.storage.data_structure_client import DataStructureClient
from jiffy.directory.directory_client import ReplicaChain
from jiffy.directory.ttypes import rpc_storage_mode


class FileOps:
    write = b('write')
    read = b('read')
    seek = b('seek')
    add_blocks = b('add_blocks')
    get_storage_capacity = b('get_storage_capacity')
    get_partition_size = b('get_partition_size')
    clear = b('clear')

    op_types = {read: CommandType.accessor,
                seek: CommandType.accessor,
                write: CommandType.mutator,
                add_blocks: CommandType.accessor,
                get_storage_capacity: CommandType.accessor,
                get_partition_size: CommandType.accessor,
                clear: CommandType.mutator}


class FileClient(DataStructureClient):
    def __init__(self, fs, path, block_info, timeout_ms):
        super(FileClient, self).__init__(fs, path, block_info, FileOps.op_types, timeout_ms)
        self.cur_partition = 0
        self.cur_offset = 0
        self.last_partition = len(self.block_info.data_blocks) - 1
        # self.last_offset = int(self.blocks[self.last_partition].run_command([FileOps.get_partition_size])[1])
        self.last_offset = 128*1024*1024
        # self.block_size = int(self.blocks[self._block_id()].run_command([FileOps.get_storage_capacity])[1])
        self.block_size = 128*1024*1024
        if self.block_info.tags.get("file.auto_scale") is None:
            self.auto_scale = True
        else:
            self.auto_scale = self.block_info.tags.get("file.auto_scale")

    def _handle_redirect(self, args, response):
        return response

    def _block_id(self):
        return self.cur_partition

    def read(self, size):
        ret = b''
        file_size = self.last_partition * self.block_size + self.last_offset
        if file_size <= self.cur_partition * self.block_size + self.cur_offset:
            raise KeyError("File offset exceeds the file size")
        remain_size = file_size - self.cur_partition * self.block_size - self.cur_offset;
        remaining_data = min(remain_size, size)
        if remaining_data == 0:
            return b""
        #Parallel read here
        start_partition = self._block_id()
        count = 0
        while remaining_data > 0:
            count += 1
            data_to_read = min(remaining_data, self.block_size - self.cur_offset)
            self.blocks[self._block_id()].send_command([FileOps.read, b(str(self.cur_offset)), b(str(data_to_read))])
            remaining_data -= data_to_read
            self.cur_offset += data_to_read
            if self.cur_offset == self.block_size and self.cur_partition != self.last_partition:
                self.cur_offset = 0
                self.cur_partition += 1
        for k in range(0, count):
            res = self.blocks[start_partition + k].recv_response()
            if b'!stale_seq_no' in res:
                raise RuntimeError('Stale sequence number')
            ret += res[-1]
        return ret

    def write(self, data):
        file_size = (self.last_partition + 1) * self.block_size
        num_chain_needed = 0
        if self.cur_partition * self.block_size + self.cur_offset > file_size:
            num_chain_needed = int(self.cur_partition - self.last_partition)
            file_size = (self.cur_partition + 1) * self.block_size
            remain_size = file_size - self.cur_partition * self.block_size - self.cur_offset
            num_chain_needed += int((len(data) - remain_size) / self.block_size + ((len(data) - remain_size) % self.block_size != 0))
        else:
            remain_size = file_size - self.cur_partition * self.block_size - self.cur_offset
            if remain_size < len(data):
                num_chain_needed = int((len(data) - remain_size) / self.block_size + ((len(data) - remain_size) % self.block_size != 0))

        if num_chain_needed and not self.auto_scale:
            return -1
        # First allocate new blocks if needed
        while num_chain_needed != 0:
            _return = self.blocks[self.last_partition].run_command([FileOps.add_blocks, b(str(self.last_partition)), b(str(num_chain_needed))])
            if _return[0] == b("!block_allocated"):
                self.last_partition += num_chain_needed
                self.last_offset = 0
                num_chain_needed = 0
                try:
                    for x in _return[1:]:
                        block_ids = [bytes_to_str(j) for j in x.split(b('!'))]
                        chain = ReplicaChain(block_ids, 0, 0, rpc_storage_mode.rpc_in_memory)
                        self.blocks.append(
                            ReplicaChainClient(self.fs, self.path, self.client_cache, chain, FileOps.op_types))
                except:
                    return -1
        if self.block_size == self.cur_offset:
            self.cur_offset = 0
            self.cur_partition += 1
        # Parallel write
        remaining_data = len(data)
        start_partition = self._block_id()
        count = 0
        while remaining_data > 0:
            count += 1
            data_to_write = data[len(data) - remaining_data : len(data) - remaining_data + min(remaining_data, self.block_size - self.cur_offset)]
            self.blocks[self._block_id()].send_command([FileOps.write, data_to_write, b(str(self.cur_offset))])
            remaining_data -= len(data_to_write)
            self.cur_offset += len(data_to_write)
            if self.last_offset < self.cur_offset and self.cur_partition == self.last_partition:
                self.last_offset = self.cur_offset
            if self.cur_offset == self.block_size and self.cur_partition != self.last_partition:
                self.cur_offset = 0
                self.cur_partition += 1
                if self.last_partition < self.cur_partition:
                    self.last_partition = self.cur_partition
                    self.last_offset = self.cur_offset
        for i in range(0, count):
            res = self.blocks[start_partition + i].recv_response()
            if b'!stale_seq_no' in res:
                raise RuntimeError('Stale sequence number')

        return len(data)

    def seek(self, offset):
        self.cur_partition = int(offset / self.block_size)
        self.cur_offset = int(offset % self.block_size)
        return True

    def clear(self):
        for i in range(len(self.block_info.data_blocks)):
            self.blocks[i].run_command([FileOps.clear])
