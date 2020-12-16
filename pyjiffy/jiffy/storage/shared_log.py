from jiffy.storage.replica_chain_client import ReplicaChainClient
from jiffy.storage.command import CommandType
from jiffy.storage.compat import b, unicode, bytes, long, basestring, bytes_to_str
from jiffy.storage.data_structure_client import DataStructureClient
from jiffy.directory.directory_client import ReplicaChain
from jiffy.directory.ttypes import rpc_storage_mode


class SharedLogOps:
    write = b('write')
    scan = b('scan')
    trim = b('trim')
    add_blocks = b('add_blocks')
    get_storage_capacity = b('get_storage_capacity')

    op_types = {scan: CommandType.accessor,
                trim: CommandType.mutator,
                write: CommandType.mutator,
                add_blocks: CommandType.accessor,
                get_storage_capacity: CommandType.accessor}


class SharedLogClient(DataStructureClient):
    def __init__(self, fs, path, block_info, timeout_ms):
        super(SharedLogClient, self).__init__(fs, path, block_info, SharedLogOps.op_types, timeout_ms)
        self.cur_partition = 0
        self.cur_offset = 0
        self.last_partition = len(self.block_info.data_blocks) - 1
        self.last_offset = 0
        self.block_size = int(self.blocks[self._block_id()].run_command([SharedLogOps.get_storage_capacity])[1])
        if self.block_info.tags.get("shared_log.auto_scale") is None:
            self.auto_scale = True
        else:
            self.auto_scale = self.block_info.tags.get("shared_log.auto_scale")

    def _handle_redirect(self, args, response):
        return response

    def _block_id(self):
        return self.cur_partition

    def scan(self, start_pos, end_pos, logical_streams):
        start_partition = 0
        count = 0
        
        while start_partition + count < len(self.block_info.data_blocks):
            arg_list = [SharedLogOps.scan, b(str(start_pos)), b(str(end_pos))]
            arg_list += logical_streams
            self.blocks[start_partition + count].send_command(arg_list)
            count += 1
        ret = []
        for k in range(0, count):
            resp = self.blocks[start_partition + k].recv_response()
            if len(resp) > 1:
                ret += resp[1:]
        return ret

    def write(self, pos, data_, logical_streams):
        file_size = (self.last_partition + 1) * self.block_size
        num_chain_needed = 0

        data = ""
        for ls in logical_streams:
            data += ls
        data += data_

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
            _return = self.blocks[self.last_partition].run_command([SharedLogOps.add_blocks, b(str(self.last_partition)), b(str(num_chain_needed))])
            if _return[0] == b("!block_allocated"):
                self.last_partition += num_chain_needed
                self.last_offset = 0
                num_chain_needed = 0
                try:
                    for x in _return[1:]:
                        block_ids = [bytes_to_str(j) for j in x.split(b('!'))]
                        chain = ReplicaChain(block_ids, 0, 0, rpc_storage_mode.rpc_in_memory)
                        self.blocks.append(
                            ReplicaChainClient(self.fs, self.path, self.client_cache, chain, SharedLogOps.op_types))
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
            if len(data > self.block_size - self.cur_offset): 
                self.cur_offset = 0
                self.cur_partition += 1
                if self.last_partition < self.cur_partition:
                    self.last_partition = self.cur_partition
                    self.last_offset = self.cur_offset
            arg_list = [SharedLogOps.write, b(str(pos)), data_]
            arg_list += logical_streams
            
            self.blocks[self._block_id()].send_command(arg_list)
            remaining_data -= len(data)
            self.cur_offset += len(data)
            if self.last_offset < self.cur_offset and self.cur_partition == self.last_partition:
                self.last_offset = self.cur_offset
            
        for i in range(0, count):
            self.blocks[start_partition + i].recv_response()

        return len(data)

    def trim(self, start_pos, end_pos):
        start_partition = 0
        count = 0
        while start_partition + count < len(self.block_info.data_blocks):
            arg_list = [SharedLogOps.trim, b(str(start_pos)), b(str(end_pos))]
            self.blocks[start_partition + count].send_command(arg_list)
            count += 1
        ret = []
        for k in range(0, count):
            resp = self.blocks[start_partition + k].recv_response()
            
        return True