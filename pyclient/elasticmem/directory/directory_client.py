from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport import TTransport, TSocket

import directory_rpc_service


class Perms:
    def __init__(self):
        pass

    none = 0

    owner_read = 0400
    owner_write = 0200
    owner_exec = 0100
    owner_all = 0700

    group_read = 040
    group_write = 020
    group_exec = 010
    group_all = 070

    others_read = 04
    others_write = 02
    others_exec = 01
    others_all = 07

    all = 0777

    set_uid = 04000
    set_gid = 02000
    sticky_bit = 01000

    mask = 07777


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


class DataStatus:
    def __init__(self, storage_mode, persistent_store_prefix, data_blocks):
        self.storage_mode = storage_mode
        self.persistent_store_prefix = persistent_store_prefix
        self.data_blocks = data_blocks

    def get_storage_mode(self):
        return self.storage_mode

    def get_persistent_store_prefix(self):
        return self.persistent_store_prefix

    def get_data_blocks(self):
        return self.data_blocks


class DirectoryClient:
    def __init__(self, host='localhost', port=9090):
        self.socket_ = TSocket.TSocket(host, port)
        self.transport_ = TTransport.TBufferedTransport(self.socket_)
        self.protocol_ = TBinaryProtocol(self.transport_)
        self.client_ = directory_rpc_service.Client(self.protocol_)
        self.transport_.open()

    def __del__(self):
        self.close()

    def close(self):
        if self.transport_.isOpen():
            self.transport_.close()

    def create_directory(self, path):
        self.client_.create_directory(path)

    def create_directories(self, path):
        self.client_.create_directories(path)

    def create_file(self, path, persistent_store_prefix):
        self.client_.create_file(path, persistent_store_prefix)

    def exists(self, path):
        return self.client_.exists(path)

    def file_size(self, path):
        return self.client_.file_size(path)

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
        return DataStatus(s.storage_mode, s.persistent_store_prefix, s.data_blocks)

    def mode(self, path):
        return self.client_.mode(path)

    def persistent_store_prefix(self, path):
        return self.client_.persistent_store_prefix(path)

    def data_blocks(self, path):
        return self.client_.data_blocks(path)

    def is_regular_file(self, path):
        return self.client_.is_regular_file(path)

    def is_directory(self, path):
        return self.client_.is_directory(path)
