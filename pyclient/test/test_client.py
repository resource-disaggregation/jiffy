from __future__ import print_function

import os
import subprocess
import sys
import tempfile
import time

try:
    import Queue as queue
except ImportError:
    import queue
from unittest import TestCase

from thrift.transport import TTransport, TSocket

from elasticmem import ElasticMemClient, RemoveMode, StorageMode
from elasticmem.benchmark.kv_async_benchmark import run_async_kv_benchmark
from elasticmem.benchmark.kv_sync_benchmark import run_sync_kv_throughput_benchmark, run_sync_kv_latency_benchmark
from elasticmem.subscription.subscriber import Notification


def wait_till_server_ready(host, port):
    check = True
    while check:
        try:
            transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
            transport.open()
            transport.close()
            check = False
        except TTransport.TTransportException:
            time.sleep(0.1)


def gen_async_kv_ops():
    tf = tempfile.NamedTemporaryFile(delete=False)
    with open(tf.name, "w+") as f:
        for i in range(0, 1000):
            f.write("%s %d %d\n" % ("put", i, i))
        for i in range(0, 1000):
            f.write("%s %d\n" % ("get", i))
        for i in range(0, 1000):
            f.write("%s %d %d\n" % ("update", i, i + 1000))
        for i in range(0, 1000):
            f.write("%s %d\n" % ("get", i))
        for i in range(0, 1000):
            f.write("%s %d\n" % ("remove", i))
    return tf.name


class TestClient(TestCase):
    DIRECTORY_SERVER_EXECUTABLE = os.getenv('DIRECTORY_SERVER_EXEC', 'directoryd')
    STORAGE_SERVER_EXECUTABLE = os.getenv('STORAGE_SERVER_EXEC', 'storaged')
    STORAGE_HOST = '127.0.0.1'
    STORAGE_SERVICE_PORT = 9093
    STORAGE_MANAGEMENT_PORT = 9094
    STORAGE_NOTIFICATION_PORT = 9095
    STORAGE_CHAIN_PORT = 9096
    DIRECTORY_HOST = '127.0.0.1'
    DIRECTORY_SERVICE_PORT = 9090
    DIRECTORY_LEASE_PORT = 9091

    def start_servers(self, directoryd_args=None, storaged_args=None):
        if storaged_args is None:
            storaged_args = []
        if directoryd_args is None:
            directoryd_args = []
        try:
            self.directoryd = subprocess.Popen([self.DIRECTORY_SERVER_EXECUTABLE] + directoryd_args)
        except OSError as e:
            print("Error running executable %s: %s" % (self.DIRECTORY_SERVER_EXECUTABLE, e))
            sys.exit()

        wait_till_server_ready(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT)
        wait_till_server_ready(self.DIRECTORY_HOST, self.DIRECTORY_LEASE_PORT)

        try:
            self.storaged = subprocess.Popen([self.STORAGE_SERVER_EXECUTABLE] + storaged_args)
        except OSError as e:
            print("Error running executable %s: %s" % (self.STORAGE_SERVER_EXECUTABLE, e))
            sys.exit()

        wait_till_server_ready(self.STORAGE_HOST, self.STORAGE_SERVICE_PORT)
        wait_till_server_ready(self.STORAGE_HOST, self.STORAGE_MANAGEMENT_PORT)
        wait_till_server_ready(self.STORAGE_HOST, self.STORAGE_NOTIFICATION_PORT)
        wait_till_server_ready(self.STORAGE_HOST, self.STORAGE_CHAIN_PORT)

    def stop_servers(self):
        self.directoryd.kill()
        self.directoryd.wait()
        self.storaged.kill()
        self.storaged.wait()

    def kv_ops(self, kv):
        # Test get/put
        for i in range(0, 1000):
            self.assertTrue(kv.put(str(i), str(i)) == 'ok')

        for i in range(0, 1000):
            self.assertTrue(kv.get(str(i)) == bytes(str(i), 'utf-8'))

        for i in range(1000, 2000):
            self.assertTrue(kv.get(str(i)) is None)

        self.assertTrue(kv.num_keys() == 1000)

        # Test update
        for i in range(0, 1000):
            self.assertTrue(kv.update(str(i), str(i + 1000)) == bytes(str(i), 'utf-8'))

        for i in range(1000, 2000):
            self.assertTrue(kv.update(str(i), str(i + 1000)) is None)

        for i in range(0, 1000):
            self.assertTrue(kv.get(str(i)) == bytes(str(i + 1000), 'utf-8'))

        self.assertTrue(kv.num_keys() == 1000)

        # Test remove
        for i in range(0, 1000):
            self.assertTrue(kv.remove(str(i)) == bytes(str(i + 1000), 'utf-8'))

        for i in range(1000, 2000):
            self.assertTrue(kv.remove(str(i)) is None)

        for i in range(0, 1000):
            self.assertTrue(kv.get(str(i)) is None)

        self.assertTrue(kv.num_keys() == 0)

    def pipelined_kv_ops(self, kv):
        # Test get/put
        puts = kv.pipeline_put()
        gets = kv.pipeline_get()
        removes = kv.pipeline_remove()
        updates = kv.pipeline_update()

        for i in range(0, 1000):
            puts.put(str(i), str(i))
        self.assertTrue(puts.execute() == [b'!ok'] * 1000)

        for i in range(0, 1000):
            gets.get(str(i))
        self.assertTrue(gets.execute() == [bytes(str(i), 'utf-8') for i in range(0, 1000)])

        for i in range(1000, 2000):
            gets.get(str(i))
        self.assertTrue(gets.execute() == [b'!key_not_found'] * 1000)

        self.assertTrue(kv.num_keys() == 1000)

        # Test update
        for i in range(0, 1000):
            updates.update(str(i), str(i + 1000))
        self.assertTrue(updates.execute() == [bytes(str(i), 'utf-8') for i in range(0, 1000)])

        for i in range(1000, 2000):
            updates.update(str(i), str(i + 1000))
        self.assertTrue(updates.execute() == [b'!key_not_found'] * 1000)

        for i in range(0, 1000):
            gets.get(str(i))
        self.assertTrue(gets.execute() == [bytes(str(i + 1000), 'utf-8') for i in range(0, 1000)])

        self.assertTrue(kv.num_keys() == 1000)

        # Test remove
        for i in range(0, 1000):
            removes.remove(str(i))
        self.assertTrue(removes.execute() == [bytes(str(i + 1000), 'utf-8') for i in range(0, 1000)])

        for i in range(1000, 2000):
            removes.remove(str(i))
        self.assertTrue(removes.execute() == [b'!key_not_found'] * 1000)

        for i in range(0, 1000):
            gets.get(str(i))
        self.assertTrue(gets.execute() == [b'!key_not_found'] * 1000)

        self.assertTrue(kv.num_keys() == 0)

    def test_lease_worker(self):
        self.start_servers()
        client = ElasticMemClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT)
        try:
            client.create("/a/file.txt", "/tmp")
            self.assertTrue(client.fs.exists("/a/file.txt"))
            time.sleep(client.lease_worker.renewal_duration_s)
            self.assertTrue(client.fs.exists("/a/file.txt"))
            time.sleep(client.lease_worker.renewal_duration_s)
            self.assertTrue(client.fs.exists("/a/file.txt"))
        finally:
            client.disconnect()
            self.stop_servers()

    def test_create(self):
        self.start_servers()
        client = ElasticMemClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT)
        try:
            kv = client.create("/a/file.txt", "/tmp")
            self.kv_ops(kv)
            self.pipelined_kv_ops(kv)
            self.assertTrue(client.fs.exists('/a/file.txt'))
        finally:
            client.disconnect()
            self.stop_servers()

    def test_open(self):
        self.start_servers()
        client = ElasticMemClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT)
        try:
            client.create("/a/file.txt", "/tmp")
            self.assertTrue(client.fs.exists('/a/file.txt'))
            kv = client.open('/a/file.txt')
            self.kv_ops(kv)
            self.pipelined_kv_ops(kv)
        finally:
            client.disconnect()
            self.stop_servers()

    def test_close(self):
        self.start_servers()
        client = ElasticMemClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT)
        try:
            client.create("/a/file.txt", "/tmp")
            self.assertTrue('/a/file.txt' in client.to_renew)
            client.remove('/a/file.txt', RemoveMode.flush)
            self.assertFalse('/a/file.txt' in client.to_renew)
            self.assertTrue(client.fs.dstatus('/a/file.txt').storage_mode == StorageMode.on_disk)
            client.remove('/a/file.txt', RemoveMode.delete)
            self.assertFalse('/a/file.txt' in client.to_renew)
            self.assertFalse(client.fs.exists('/a/file.txt'))
        finally:
            client.disconnect()
            self.stop_servers()

    def test_auto_scale(self):
        self.start_servers(None, ["--block-capacity", "7705"])
        client = ElasticMemClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT)
        try:
            kv = client.create("/a/file.txt", "/tmp")
            for i in range(0, 2000):
                self.assertTrue(kv.put(str(i), str(i)) == 'ok')
            self.assertTrue(len(client.fs.dstatus("/a/file.txt").data_blocks) == 4)
            for i in range(0, 2000):
                self.assertTrue(kv.remove(str(i)) == bytes(str(i), 'utf-8'))
            self.assertTrue(len(client.fs.dstatus("/a/file.txt").data_blocks) == 1)
        finally:
            client.disconnect()
            self.stop_servers()

    def test_notifications(self):
        self.start_servers()
        client = ElasticMemClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT)
        try:
            client.fs.create("/a/file.txt", "/tmp")

            n1 = client.open_listener("/a/file.txt")
            n2 = client.open_listener("/a/file.txt")
            n3 = client.open_listener("/a/file.txt")

            n1.subscribe(['put'])
            n2.subscribe(['put', 'remove'])
            n3.subscribe(['remove'])

            kv = client.open("/a/file.txt")
            kv.put('key1', 'value1')
            kv.remove('key1')

            self.assertTrue(n1.get_notification() == Notification('put', b'key1'))
            n2_notifs = [n2.get_notification(), n2.get_notification()]
            self.assertTrue(Notification('put', b'key1') in n2_notifs)
            self.assertTrue(Notification('remove', b'key1') in n2_notifs)
            self.assertTrue(n3.get_notification() == Notification('remove', b'key1'))

            with self.assertRaises(queue.Empty):
                n1.get_notification(block=False)
            with self.assertRaises(queue.Empty):
                n2.get_notification(block=False)
            with self.assertRaises(queue.Empty):
                n3.get_notification(block=False)

            n1.unsubscribe(['put'])
            n2.unsubscribe(['remove'])

            kv.put('key1', 'value1')
            kv.remove('key1')

            self.assertTrue(n2.get_notification() == Notification('put', b'key1'))
            self.assertTrue(n3.get_notification() == Notification('remove', b'key1'))

            with self.assertRaises(queue.Empty):
                n1.get_notification(block=False)
            with self.assertRaises(queue.Empty):
                n2.get_notification(block=False)
            with self.assertRaises(queue.Empty):
                n3.get_notification(block=False)

            n1.disconnect()
            n2.disconnect()
            n3.disconnect()
        finally:
            client.disconnect()
            self.stop_servers()

    def test_benchmark(self):
        self.start_servers()

        # Setup: create workload file
        workload_path = gen_async_kv_ops()
        client = ElasticMemClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT)
        try:
            data_path1 = "/a/file1.txt"
            client.fs.create(data_path1, "/tmp")
            run_async_kv_benchmark(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT,
                                   data_path1, workload_path)

            data_path2 = "/a/file2.txt"
            client.fs.create(data_path2, "/tmp")
            run_sync_kv_throughput_benchmark(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT,
                                             self.DIRECTORY_LEASE_PORT, data_path2, workload_path)

            data_path3 = "/a/file3.txt"
            client.fs.create(data_path3, "/tmp")
            run_sync_kv_latency_benchmark(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT,
                                          data_path3, workload_path)
        finally:
            client.disconnect()
            self.stop_servers()
