from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated
from thrift.transport import TTransport, TSocket
from thrift.transport.TTransport import TTransportException

from elasticmem.directory import directory_service


class Perms:
    def __init__(self):
        pass

    none = 0

    owner_read = 0o400
    owner_write = 0o200
    owner_exec = 0o100
    owner_all = 0o700

    group_read = 0o40
    group_write = 0o20
    group_exec = 0o10
    group_all = 0o70

    others_read = 0o4
    others_write = 0o2
    others_exec = 0o1
    others_all = 0o7

    all = 0o777

    set_uid = 0o4000
    set_gid = 0o2000
    sticky_bit = 0o1000

    mask = 0o7777


class PermOpts:
    def __init__(self):
        pass

    replace = 0
    add = 1
    remove = 2


class DirectoryEntry:
    def __init__(self, name, status):
        self.name = name
        self.status = status

    def get_name(self):
        return self.name

    def get_status(self):
        return self.status


class FileStatus:
    def __init__(self, file_type, permissions, last_write_time):
        self.type = file_type
        self.permissions = permissions
        self.last_write_time = last_write_time

    def get_type(self):
        return self.type

    def get_permissions(self):
        return self.permissions

    def get_last_write_time(self):
        return self.last_write_time


class StorageMode:
    def __init__(self):
        pass

    in_memory = 0
    in_memory_grace = 1
    flushing = 2
    on_disk = 3


class BlockChain:
    def __init__(self, block_names):
        self.block_names = block_names


class DataStatus:
    def __init__(self, storage_mode, persistent_store_prefix, chain_length, data_blocks):
        self.storage_mode = storage_mode
        self.persistent_store_prefix = persistent_store_prefix
        self.chain_length = chain_length
        self.data_blocks = [BlockChain(block_chain.block_names) for block_chain in data_blocks]


class DirectoryClient:
    def __init__(self, host='127.0.0.1', port=9090):
        self.socket_ = TSocket.TSocket(host, port)
        self.transport_ = TTransport.TBufferedTransport(self.socket_)
        self.protocol_ = TBinaryProtocolAccelerated(self.transport_)
        self.client_ = directory_service.Client(self.protocol_)
        ex = None
        for i in range(3):
            try:
                self.transport_.open()
            except TTransportException as e:
                ex = e
                continue
            except Exception:
                raise
        else:
            raise TTransportException(ex.type, "Connection failed {}:{}: {}".format(host, port, ex.message))



    def __del__(self):
        self.close()

    def close(self):
        if self.transport_.isOpen():
            self.transport_.close()

    def create_directory(self, path):
        self.client_.create_directory(path)

    def create_directories(self, path):
        self.client_.create_directories(path)

    def open(self, path):
        s = self.client_.open(path)
        return DataStatus(s.storage_mode, s.persistent_store_prefix, s.chain_length, s.data_blocks)

    def create(self, path, persistent_store_prefix, num_blocks=1, chain_length=1):
        s = self.client_.create(path, persistent_store_prefix, num_blocks, chain_length)
        return DataStatus(s.storage_mode, s.persistent_store_prefix, s.chain_length, s.data_blocks)

    def open_or_create(self, path, persistent_store_prefix, num_blocks=1, chain_length=1):
        s = self.client_.open_or_create(path, persistent_store_prefix, num_blocks, chain_length)
        return DataStatus(s.storage_mode, s.persistent_store_prefix, s.chain_length, s.data_blocks)

    def exists(self, path):
        return self.client_.exists(path)

    def last_write_time(self, path):
        return self.client_.last_write_time(path)

    def set_permissions(self, path, prms, opts):
        self.client_.set_permissions(path, prms, opts)

    def get_permissions(self, path):
        return self.client_.get_permissions(path)

    def remove(self, path):
        self.client_.remove(path)

    def remove_all(self, path):
        self.client_.remove_all(path)

    def flush(self, path):
        self.client_.flush(path)

    def status(self, path):
        s = self.client_.status(path)
        return FileStatus(s.type, s.permissions, s.last_write_time)

    def directory_entries(self, path):
        entries = self.client_.directory_entries(path)
        return [DirectoryEntry(e.name, e.status) for e in entries]

    def recursive_directory_entries(self, path):
        entries = self.client_.recursive_directory_entries(path)
        return [DirectoryEntry(e.name, e.status) for e in entries]

    def dstatus(self, path):
        s = self.client_.dstatus(path)
        return DataStatus(s.storage_mode, s.persistent_store_prefix, s.chain_length, s.data_blocks)

    def is_regular_file(self, path):
        return self.client_.is_regular_file(path)

    def is_directory(self, path):
        return self.client_.is_directory(path)

    def resolve_failures(self, path, chain):
        self.client_.reslove_failures(path, chain)

    def add_blocks_to_chain(self, path, chain, count):
        self.client_.add_blocks(path, chain, count)
