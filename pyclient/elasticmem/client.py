import threading

import time

from directory.directory_client import DirectoryClient
from directory.lease_client import LeaseClient
from elasticmem.directory.ttypes import rpc_file_metadata
from kv.kv_client import KVClient


class RemoveMode:
    def __init__(self):
        pass

    delete = 0
    flush = 1


def now_ms():
    return long(round(time.time() * 1000))


class LeaseRenewalWorker(threading.Thread):
    def __init__(self, renewal_duration_ms, ls, kvs, tot_bytes, to_renew, to_flush, to_remove):
        self.renewal_duration_s = float(renewal_duration_ms) / 1000.0
        self.ls = ls
        self.kvs = kvs
        self.tot_bytes = tot_bytes
        self.to_renew = to_renew
        self.to_flush = to_flush
        self.to_remove = to_remove
        super(LeaseRenewalWorker, self).__init__()

    def run(self):
        while True:
            s = now_ms()
            to_renew_metadata = [rpc_file_metadata(path, self.kvs[path].bytes_written) for path in self.to_renew]
            ack = self.ls.update_lease(to_renew_metadata, self.to_flush, self.to_remove)
            self.tot_bytes = {entry.path: entry.bytes for entry in ack.renewed}
            del self.to_flush[:]
            del self.to_remove[:]
            elapsed_s = float(now_ms() - s) / 1000.0
            sleep_time = self.renewal_duration_s - elapsed_s
            if sleep_time > 0.0:
                time.sleep(sleep_time)


class ElasticMemClient:
    def __init__(self, host="127.0.0.1", service_port=9090, lease_port=9091, renewal_duration_ms=10000):
        self.directory_host = host
        self.directory_port = service_port
        self.ds = DirectoryClient(host, service_port)
        self.kvs = {}
        self.paths_to_renew = []
        self.paths_to_flush = []
        self.paths_to_remove = []
        self.ls = LeaseClient(host, lease_port)
        self.tot_bytes = {}
        self.lease_worker = LeaseRenewalWorker(renewal_duration_ms, self.ls, self.kvs, self.tot_bytes,
                                               self.paths_to_renew, self.paths_to_flush, self.paths_to_remove)
        self.lease_worker.daemon = True
        self.lease_worker.start()

    def __del__(self):
        self.close()

    def close(self):
        self.ds.close()
        self.ls.close()
        for kv in self.kvs.viewvalues():
            kv.close()

    def create_scope(self, path, persistent_store_prefix):
        self.ds.create_file(path, persistent_store_prefix)
        blocks = self.ds.data_blocks(path)
        print "Blocks in file %s" % path
        print blocks
        self.kvs[path] = KVClient(blocks)
        self.paths_to_renew.append(path)
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
