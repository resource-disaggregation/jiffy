from __future__ import print_function

import os
import subprocess
import sys
import tempfile
import time

try:
    import ConfigParser as configparser
except ImportError:
    import configparser
try:
    import Queue as queue
except ImportError:
    import queue
from unittest import TestCase

from thrift.transport import TTransport, TSocket

from mmux import MMuxClient, RemoveMode, StorageMode
from mmux.benchmark.kv_async_benchmark import run_async_kv_benchmark
from mmux.benchmark.kv_sync_benchmark import run_sync_kv_throughput_benchmark, run_sync_kv_latency_benchmark
from mmux.subscription.subscriber import Notification


def wait_till_server_ready(host, port):
    check = True
    while check:
        try:
            sock = TSocket.TSocket(host, port)
            sock.open()
            sock.close()
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
    def start_servers(self, chain=False, auto_scale=False):
        resource_path = os.path.join(os.path.split(__file__)[0], "resources")
        directory_conf_file = os.path.join(resource_path, "directory.conf")
        dir_exec = os.getenv('DIRECTORY_SERVER_EXEC', 'directoryd')
        try:
            self.directoryd = subprocess.Popen([dir_exec, '--config', directory_conf_file])
        except OSError as e:
            print("Error running executable %s: %s" % (dir_exec, e))
            sys.exit()

        directory_conf = configparser.ConfigParser()
        directory_conf.read(directory_conf_file)
        self.dconf = directory_conf['directory']
        wait_till_server_ready(self.dconf['host'], int(self.dconf['service_port']))
        wait_till_server_ready(self.dconf['host'], int(self.dconf['lease_port']))
        wait_till_server_ready(self.dconf['host'], int(self.dconf['block_port']))

        storage_exec = os.getenv('STORAGE_SERVER_EXEC', 'storaged')
        if auto_scale:
            storage1_conf_file = os.path.join(resource_path, "storage_auto_scale.conf")
        else:
            storage1_conf_file = os.path.join(resource_path, "storage1.conf")
        try:
            self.storaged = subprocess.Popen([storage_exec, "--config", storage1_conf_file])
        except OSError as e:
            print("Error running executable %s: %s" % (storage_exec, e))
            sys.exit()

        storage1_conf = configparser.ConfigParser()
        storage1_conf.read(storage1_conf_file)
        self.s1conf = storage1_conf['storage']
        wait_till_server_ready(self.s1conf['host'], int(self.s1conf['service_port']))
        wait_till_server_ready(self.s1conf['host'], int(self.s1conf['management_port']))
        wait_till_server_ready(self.s1conf['host'], int(self.s1conf['notification_port']))
        wait_till_server_ready(self.s1conf['host'], int(self.s1conf['chain_port']))

        if chain:
            storage2_conf_file = os.path.join(resource_path, "storage2.conf")
            try:
                self.storaged2 = subprocess.Popen([storage_exec, "--config", storage2_conf_file])
            except OSError as e:
                print("Error running executable %s: %s" % (storage_exec, e))
                sys.exit()

            storage2_conf = configparser.ConfigParser()
            storage2_conf.read(storage2_conf_file)
            self.s2conf = storage2_conf['storage']
            wait_till_server_ready(self.s2conf['host'], int(self.s2conf['service_port']))
            wait_till_server_ready(self.s2conf['host'], int(self.s2conf['management_port']))
            wait_till_server_ready(self.s2conf['host'], int(self.s2conf['notification_port']))
            wait_till_server_ready(self.s2conf['host'], int(self.s2conf['chain_port']))

            storage3_conf_file = os.path.join(resource_path, "storage3.conf")
            try:
                self.storaged3 = subprocess.Popen([storage_exec, "--config", storage3_conf_file])
            except OSError as e:
                print("Error running executable %s: %s" % (storage_exec, e))
                sys.exit()

            storage3_conf = configparser.ConfigParser()
            storage3_conf.read(storage3_conf_file)
            self.s3conf = storage3_conf['storage']
            wait_till_server_ready(self.s3conf['host'], int(self.s3conf['service_port']))
            wait_till_server_ready(self.s3conf['host'], int(self.s3conf['management_port']))
            wait_till_server_ready(self.s3conf['host'], int(self.s3conf['notification_port']))
            wait_till_server_ready(self.s3conf['host'], int(self.s3conf['chain_port']))
        else:
            self.storaged2 = None
            self.storaged3 = None

    def stop_servers(self):
        self.directoryd.kill()
        self.directoryd.wait()
        self.storaged.kill()
        self.storaged.wait()
        if self.storaged2 is not None:
            self.storaged2.kill()
            self.storaged2.wait()
        if self.storaged3 is not None:
            self.storaged3.kill()
            self.storaged3.wait()

    def kv_ops(self, kv):
        # Test exists/get/put
        for i in range(0, 1000):
            self.assertTrue(kv.put(str(i), str(i)) == 'ok')

        for i in range(0, 1000):
            self.assertTrue(kv.exists(str(i)))
            self.assertTrue(kv.get(str(i)) == bytes(str(i), 'utf-8'))

        for i in range(1000, 2000):
            self.assertFalse(kv.exists(str(i)))
            self.assertTrue(kv.get(str(i)) is None)

        self.assertTrue(kv.num_keys() == 1000)

        # Test keys
        keys = sorted([int(k) for k in kv.keys()])
        for i in range(0, 1000):
            self.assertTrue(keys[i] == i)

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
        client = MMuxClient(self.dconf['host'], int(self.dconf['service_port']), int(self.dconf['lease_port']))
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
        client = MMuxClient(self.dconf['host'], int(self.dconf['service_port']), int(self.dconf['lease_port']))
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
        client = MMuxClient(self.dconf['host'], int(self.dconf['service_port']), int(self.dconf['lease_port']))
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
        client = MMuxClient(self.dconf['host'], int(self.dconf['service_port']), int(self.dconf['lease_port']))
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

    def test_chain_replication(self):  # TODO: Add failure tests
        self.start_servers(chain=True)
        client = MMuxClient(self.dconf['host'], int(self.dconf['service_port']), int(self.dconf['lease_port']))
        try:
            kv = client.create("/a/file.txt", "/tmp", 1, 3)
            self.assertTrue(kv.file_info.chain_length == 3)
            self.kv_ops(kv)
            self.pipelined_kv_ops(kv)
        finally:
            client.disconnect()
            self.stop_servers()

    def test_auto_scale(self):
        self.start_servers(auto_scale=True)
        client = MMuxClient(self.dconf['host'], int(self.dconf['service_port']), int(self.dconf['lease_port']))
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
        client = MMuxClient(self.dconf['host'], int(self.dconf['service_port']), int(self.dconf['lease_port']))
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
        client = MMuxClient(self.dconf['host'], int(self.dconf['service_port']), int(self.dconf['lease_port']))
        try:
            data_path1 = "/a/file1.txt"
            client.fs.create(data_path1, "/tmp")
            run_async_kv_benchmark(self.dconf['host'], int(self.dconf['service_port']), int(self.dconf['lease_port']),
                                   data_path1, workload_path)

            data_path2 = "/a/file2.txt"
            client.fs.create(data_path2, "/tmp")
            run_sync_kv_throughput_benchmark(self.dconf['host'], int(self.dconf['service_port']),
                                             int(self.dconf['lease_port']), data_path2, workload_path)

            data_path3 = "/a/file3.txt"
            client.fs.create(data_path3, "/tmp")
            run_sync_kv_latency_benchmark(self.dconf['host'], int(self.dconf['service_port']),
                                          int(self.dconf['lease_port']), data_path3, workload_path)
        finally:
            client.disconnect()
            self.stop_servers()
