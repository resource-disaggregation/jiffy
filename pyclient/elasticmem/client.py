import threading

import time

from directory.directory_client import DirectoryClient
from directory.lease_client import LeaseClient
from kv.kv_client import KVClient


class RemoveMode:
    def __init__(self):
        pass

    delete = 0
    flush = 1


def now_ms():
    return long(round(time.time() * 1000))


class LeaseRenewalWorker(threading.Thread):
    def __init__(self, renewal_duration_ms, host, port, kvs, tot_bytes, to_renew, to_flush, to_remove):
        self.renewal_duration_s = renewal_duration_ms / 1000.0
        self.ls = LeaseClient(host, port)
        self.kvs = kvs
        self.tot_bytes = tot_bytes
        self.to_renew = to_renew
        self.to_flush = to_flush
        self.to_remove = to_remove
        super(LeaseRenewalWorker, self).__init__()

    def run(self):
        while True:
            s = now_ms()
            to_renew_metadata = [(path, self.kvs[path].bytes_written) for path in self.to_renew]
            ack = self.ls.update_lease(to_renew_metadata, self.to_flush, self.to_remove)
            self.tot_bytes = {entry.path: entry.bytes for entry in ack.renewed}
            sleep_time = self.renewal_duration_s - (now_ms() - s)
            time.sleep(sleep_time)


class ElasticMemClient:
    def __init__(self, host="localhost", port=9090, renewal_duration_ms=100):
        self.directory_host = host
        self.directory_port = port
        self.ds = DirectoryClient(host, port)
        self.kvs = {}
        self.paths_to_renew = []
        self.paths_to_flush = []
        self.paths_to_remove = []
        self.tot_bytes = {}
        self.lease_worker = LeaseRenewalWorker(renewal_duration_ms, host, port, self.kvs, self.tot_bytes,
                                               self.paths_to_renew, self.paths_to_flush, self.paths_to_remove)
        self.lease_worker.daemon = True
        self.lease_worker.start()

    def __del__(self):
        self.ds.close()

    def create_scope(self, path, persistent_store_prefix):
        self.ds.create_file(path, persistent_store_prefix)
        blocks = self.ds.data_blocks(path)
        self.paths_to_renew.append(path)
        self.kvs[path] = KVClient(blocks)
        return self.kvs[path]

    def get_scope(self, path):
        if path in self.kvs:
            return self.kvs[path]
        blocks = self.ds.data_blocks(path)
        if path not in self.paths_to_renew:
            self.paths_to_renew.append(path)
        self.kvs[path] = KVClient(blocks)
        return self.kvs[path]

    def destroy_scope(self, path, mode):
        if path in self.paths_to_renew:
            self.paths_to_renew.remove(path)
        if path in self.kvs:
            self.kvs[path].close()
            del self.kvs[path]
        if mode == RemoveMode.delete:
            self.paths_to_remove.append(path)
        elif mode == RemoveMode.flush:
            self.paths_to_flush.append(path)
