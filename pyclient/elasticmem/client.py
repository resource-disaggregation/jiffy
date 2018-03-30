import threading

import time

from thrift.transport.TTransport import TTransportException

from directory.directory_client import DirectoryClient
from elasticmem.subscription.subscriber import SubscriptionClient, Mailbox
from lease.lease_client import LeaseClient
from kv.kv_client import KVClient
import logging

logging.basicConfig(level=logging.WARN,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%d %X")

logging.getLogger('thrift').setLevel(logging.FATAL)


class RemoveMode:
    def __init__(self):
        pass

    delete = 0
    flush = 1


def now_ms():
    return long(round(time.time() * 1000))


class LeaseRenewalWorker(threading.Thread):
    def __init__(self, renewal_duration_ms, ls, kvs, to_renew, to_flush, to_remove):
        super(LeaseRenewalWorker, self).__init__()
        self.renewal_duration_s = float(renewal_duration_ms) / 1000.0
        self.ls = ls
        self.kvs = kvs
        self.to_renew = to_renew
        self.to_flush = to_flush
        self.to_remove = to_remove
        self._stop_event = threading.Event()

    def run(self):
        while not self.stopped():
            try:
                s = now_ms()
                # Only update lease if there is something to update
                if self.to_renew or self.to_flush or self.to_remove:
                    ack = self.ls.update_lease(self.to_renew, self.to_flush, self.to_remove)
                    n_renew = len(self.to_renew)
                    n_flush = len(self.to_flush)
                    n_remove = len(self.to_remove)
                    if ack.renewed != n_renew:
                        logging.warning('Asked to renew %d leases, server renewed %d leases' % (n_renew, ack.renewed))
                    if ack.flushed != n_flush:
                        logging.warning('Asked to flush %d paths, server flushed %d paths' % (n_flush, ack.flushed))
                    if ack.removed != n_remove:
                        logging.warning('Asked to remove %d paths, server removed %d paths' % (n_remove, ack.removed))
                    del self.to_flush[:]
                    del self.to_remove[:]
                elapsed_s = float(now_ms() - s) / 1000.0
                sleep_time = self.renewal_duration_s - elapsed_s
                if sleep_time > 0.0:
                    time.sleep(sleep_time)
            except TTransportException as e:
                logging.warning("Connection error: %s" % repr(e))
                break

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


class ChainFailureCallback:
    def __init__(self, fs):
        self.fs = fs

    def __call__(self, path, chain):
        self.fs.resolve_failures(path, chain)


class ElasticMemClient:
    def __init__(self, host="127.0.0.1", directory_service_port=9090, lease_port=9091, renewal_duration_ms=10000):
        self.directory_host = host
        self.directory_port = directory_service_port
        self.fs = DirectoryClient(host, directory_service_port)
        self.chain_failure_cb = ChainFailureCallback(self.fs)
        self.kvs = {}
        self.notifs = {}
        self.to_renew = []
        self.to_flush = []
        self.to_remove = []
        self.ls = LeaseClient(host, lease_port)
        self.lease_worker = LeaseRenewalWorker(renewal_duration_ms, self.ls, self.kvs, self.to_renew,
                                               self.to_flush, self.to_remove)
        self.lease_worker.daemon = True
        self.lease_worker.start()

    def __del__(self):
        self.close()

    def close(self):
        self.lease_worker.stop()
        self.fs.close()
        self.ls.close()

    def create_scope(self, path, persistent_store_prefix, num_blocks=1, chain_length=1):
        s = self.fs.create(path, persistent_store_prefix, num_blocks, chain_length)
        self.to_renew.append(path)
        self.kvs[path] = KVClient(path, s, self.chain_failure_cb)
        return self.kvs[path]

    def get_scope(self, path):
        if path in self.kvs:
            return self.kvs[path]
        s = self.fs.open(path)
        if path not in self.to_renew:
            self.to_renew.append(path)
        self.kvs[path] = KVClient(path, s, self.chain_failure_cb)
        return self.kvs[path]

    def destroy_scope(self, path, mode):
        if path in self.to_renew:
            self.to_renew.remove(path)
        if path in self.kvs:
            del self.kvs[path]
        if mode == RemoveMode.delete:
            self.to_remove.append(path)
        elif mode == RemoveMode.flush:
            self.to_flush.append(path)

    def notifications(self, path, callback=Mailbox()):
        s = self.fs.dstatus(path)
        if path not in self.to_renew:
            self.to_renew.append(path)
        return SubscriptionClient(s, callback)
