from __future__ import print_function

import os
import subprocess
import sys
import time
import threading, queue
from enum import Enum

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

from jiffy import JiffyClient, b, Flags
from jiffy.storage.subscriber import Notification

class QueryType(Enum):
    GET_QUERY = 0
    PUT_QUERY = 1

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


class JiffyServer(object):
    def __init__(self):
        self.handle = None

    def start(self, executable, conf):
        self.handle = subprocess.Popen([executable, '--config', conf])

    def stop(self):
        if self.handle is not None:
            self.handle.kill()
            self.handle.wait()
            self.handle = None


class StorageServer(JiffyServer):
    def __init__(self):
        super(StorageServer, self).__init__()
        self.host = None
        self.service_port = None
        self.management_port = None
        self.auto_scaling_port = None

    def start(self, executable, conf):
        super(StorageServer, self).start(executable, conf)
        config = configparser.ConfigParser()
        config.read(conf)
        self.host = config.get('storage', 'host')
        self.service_port = int(config.get('storage', 'service_port'))
        self.management_port = int(config.get('storage', 'management_port'))
        self.auto_scaling_port = int(config.get('storage', 'auto_scaling_port'))
        wait_till_server_ready(self.host, self.service_port)
        wait_till_server_ready(self.host, self.management_port)
        wait_till_server_ready(self.host, self.auto_scaling_port)

    def stop(self):
        super(StorageServer, self).stop()
        self.host = None
        self.service_port = None
        self.management_port = None
        self.auto_scaling_port = None


class DirectoryServer(JiffyServer):
    def __init__(self):
        super(DirectoryServer, self).__init__()
        self.host = None
        self.service_port = None
        self.lease_port = None
        self.block_port = None

    def start(self, executable, conf):
        super(DirectoryServer, self).start(executable, conf)
        config = configparser.ConfigParser()
        config.read(conf)
        self.host = config.get('directory', 'host')
        self.service_port = int(config.get('directory', 'service_port'))
        self.lease_port = int(config.get('directory', 'lease_port'))
        self.block_port = int(config.get('directory', 'block_port'))
        wait_till_server_ready(self.host, self.service_port)
        wait_till_server_ready(self.host, self.lease_port)
        wait_till_server_ready(self.host, self.block_port)

    def stop(self):
        super(DirectoryServer, self).stop()
        self.host = None
        self.service_port = None
        self.lease_port = None
        self.block_port = None

    def connect(self):
        if self.handle is None:
            raise RuntimeError("Cannot connect: server not running")

        return JiffyClient(self.host, self.service_port, self.lease_port)


class TestClient(TestCase):
    def __init__(self, *args, **kwargs):
        super(TestClient, self).__init__(*args, **kwargs)
        self.directory_server = DirectoryServer()
        self.storage_server_1 = StorageServer()
        self.storage_server_2 = StorageServer()
        self.storage_server_3 = StorageServer()

    def jiffy_client(self):
        return self.directory_server.connect()

    def start_servers(self, chain=False, auto_scale=False):
        resource_path = os.path.join(os.path.split(__file__)[0], "resources")
        directory_conf = os.path.join(resource_path, "directory.conf")
        directory_executable = os.getenv('DIRECTORY_SERVER_EXEC', 'directoryd')
        try:
            self.directory_server.start(directory_executable, directory_conf)
        except OSError as e:
            print("Error running executable %s: %s" % (directory_executable, e))
            sys.exit()

        storage_executable = os.getenv('STORAGE_SERVER_EXEC', 'storaged')
        if auto_scale:
            storage_conf_1 = os.path.join(resource_path, "storage_auto_scale.conf")
        else:
            storage_conf_1 = os.path.join(resource_path, "storage1.conf")
        try:
            self.storage_server_1.start(storage_executable, storage_conf_1)
        except OSError as e:
            print("Error running executable %s: %s" % (storage_executable, e))
            sys.exit()

        if chain:
            storage_conf_2 = os.path.join(resource_path, "storage2.conf")
            try:
                self.storage_server_2.start(storage_executable, storage_conf_2)
            except OSError as e:
                print("Error running executable %s: %s" % (storage_executable, e))
                sys.exit()

            storage_conf_3 = os.path.join(resource_path, "storage3.conf")
            try:
                self.storage_server_3.start(storage_executable, storage_conf_3)
            except OSError as e:
                print("Error running executable %s: %s" % (storage_executable, e))
                sys.exit()

    def stop_servers(self):
        self.directory_server.stop()
        self.storage_server_1.stop()
        self.storage_server_2.stop()
        self.storage_server_3.stop()

    def hash_table_ops(self, kv):
        if getattr(kv, "num_keys", None) is not None:
            self.assertEqual(0, kv.num_keys())

        # Test exists/get/put
        for i in range(0, 1000):
            try:
                kv.put(b(str(i)), b(str(i)))
            except KeyError as k:
                self.fail("Received error message: {}".format(k))

        for i in range(0, 1000):
            self.assertTrue(kv.exists(b(str(i))))
            self.assertEqual(b(str(i)), kv.get(b(str(i))))

        for i in range(1000, 2000):
            self.assertFalse(kv.exists(b(str(i))))
            self.assertRaises(KeyError, kv.get, b(str(i)))

        # Test update
        for i in range(0, 1000):
            self.assertEqual(b('!ok'), kv.update(b(str(i)), b(str(i + 1000))))

        for i in range(1000, 2000):
            self.assertRaises(KeyError, kv.update, b(str(i)), b(str(i + 1000)))

        for i in range(0, 1000):
            self.assertEqual(b(str(i + 1000)), kv.get(b(str(i))))

        # Test remove
        for i in range(0, 1000):
            self.assertEqual(b("!ok"), kv.remove(b(str(i))))

        for i in range(1000, 2000):
            self.assertEqual(b("!ok"), kv.remove(b(str(i))))

        for i in range(0, 1000):
            self.assertRaises(KeyError, kv.get, b(str(i)))

    def queue_ops(self, q):
        length = 0
        for i in range(0, 1000):
            try:
                q.put(b(str(i)))
                length += len(b(str(i)))
            except KeyError as k:
                self.fail('Received error message: {}'.format(k))

        self.assertEqual(length, q.length())
        q.in_rate()
        q.out_rate()
        for i in range(0, 1000):
            self.assertEqual(b(str(i)), q.read_next())

        for i in range(0, 1000):
            self.assertEqual(b(str(i)), q.get())
        q.in_rate()
        q.out_rate()
        self.assertEqual(0, q.length())
        for i in range(1000, 2000):
            self.assertRaises(KeyError, q.get)

    def queue_worker(self, client, query_type, result_queue, ops):
        q = client.open_queue('/a/file.txt')
        if query_type == QueryType.PUT_QUERY:
            for i in range(0, ops):
                try:
                    q.put(b(str(i)))
                except KeyError as k:
                    self.fail('Received error message: {}'.format(k))
        elif query_type == QueryType.GET_QUERY:
            for i in range(0, ops * 10):
                try:
                    q.get()
                    result_queue.put(1)
                except KeyError as k:
                    continue

    def file_ops(self, c):
        for i in range(0, 1000):
            try:
                c.write(b(str(i)))
            except KeyError as k:
                self.fail('Received error message: {}'.format(k))

        self.assertTrue(c.seek(0))

        for i in range(0, 1000):
            self.assertEqual(b(str(i)), c.read(len(b(str(i)))))

        for i in range(1000, 2000):
            self.assertRaises(KeyError, c.read, len(b(str(i))))

        self.assertTrue(c.seek(0))

        for i in range(0, 1000):
            self.assertEqual(b(str(i)), c.read(len(b(str(i)))))

    def test_lease_worker(self):
        self.start_servers()
        client = self.jiffy_client()
        try:
            client.create_hash_table("/a/file.txt", "local://tmp")
            self.assertTrue(client.fs.exists("/a/file.txt"))
            time.sleep(client.lease_worker.renewal_duration_s)
            self.assertTrue(client.fs.exists("/a/file.txt"))
            time.sleep(client.lease_worker.renewal_duration_s)
            self.assertTrue(client.fs.exists("/a/file.txt"))
        finally:
            client.disconnect()
            self.stop_servers()

    def test_hash_table(self):
        self.start_servers()
        client = self.jiffy_client()
        try:
            client.create_hash_table('/a/file.txt', 'local://tmp')
            self.assertTrue(client.fs.exists('/a/file.txt'))
            kv = client.open_hash_table('/a/file.txt')
            self.hash_table_ops(kv)
        finally:
            client.disconnect()
            self.stop_servers()

    def test_queue(self):
        self.start_servers()
        client = self.jiffy_client()
        try:
            client.create_queue('/a/file.txt', 'local://tmp')
            self.assertTrue(client.fs.exists('/a/file.txt'))
            q = client.open_queue('/a/file.txt')
            self.queue_ops(q)
        finally:
            client.disconnect()
            self.stop_servers()


    def test_file(self):
        self.start_servers()
        client = self.jiffy_client()
        try:
            c = client.create_file('/a/file.txt', 'local://tmp')
            self.assertTrue(client.fs.exists('/a/file.txt'))
            self.file_ops(c)
        finally:
            client.disconnect()
            self.stop_servers()

    def test_sync_remove(self):
        self.start_servers()
        client = self.jiffy_client()
        try:
            client.create_hash_table("/a/file.txt", "local://tmp")
            self.assertTrue('/a/file.txt' in client.to_renew)
            client.sync('/a/file.txt', 'local://tmp')
            self.assertTrue('/a/file.txt' in client.to_renew)
            client.remove('/a/file.txt')
            self.assertFalse('/a/file.txt' in client.to_renew)
            self.assertFalse(client.fs.exists('/a/file.txt'))
        finally:
            client.disconnect()
            self.stop_servers()

    def test_close(self):
        self.start_servers()
        client = self.jiffy_client()
        try:
            client.create_hash_table("/a/file.txt", "local://tmp")
            client.create_hash_table("/a/file1.txt", "local://tmp", 1, 1, Flags.pinned)
            client.create_hash_table("/a/file2.txt", "local://tmp", 1, 1, Flags.mapped)
            self.assertTrue('/a/file.txt' in client.to_renew)
            self.assertTrue('/a/file1.txt' in client.to_renew)
            self.assertTrue('/a/file2.txt' in client.to_renew)
            client.close('/a/file.txt')
            client.close('/a/file1.txt')
            client.close('/a/file2.txt')
            self.assertFalse('/a/file.txt' in client.to_renew)
            self.assertFalse('/a/file1.txt' in client.to_renew)
            self.assertFalse('/a/file2.txt' in client.to_renew)
            time.sleep(client.lease_worker.renewal_duration_s)
            self.assertTrue(client.fs.exists('/a/file.txt'))
            self.assertTrue(client.fs.exists('/a/file1.txt'))
            self.assertTrue(client.fs.exists('/a/file2.txt'))
            time.sleep(client.lease_worker.renewal_duration_s * 2)
            self.assertFalse(client.fs.exists('/a/file.txt'))
            self.assertTrue(client.fs.exists('/a/file1.txt'))
            self.assertTrue(client.fs.exists('/a/file2.txt'))
        finally:
            client.disconnect()
            self.stop_servers()

    def test_chain_replication(self):
        self.start_servers(chain=True)
        client = self.jiffy_client()
        try:
            kv = client.create_hash_table("/a/file.txt", "local://tmp", 1, 3)
            self.assertEqual(3, kv.block_info.chain_length)
            self.hash_table_ops(kv)
        finally:
            client.disconnect()
            self.stop_servers()

    def test_failures(self):
        servers = [self.storage_server_1, self.storage_server_2, self.storage_server_3]
        for s in servers:
            self.start_servers(chain=True)
            client = self.jiffy_client()
            try:
                kv = client.create_hash_table("/a/file.txt", "local://tmp", 1, 3)
                self.assertEqual(3, kv.block_info.chain_length)
                s.stop()
                self.hash_table_ops(kv)
            finally:
                client.disconnect()
                self.stop_servers()

    def test_hash_table_auto_scale(self):
        self.start_servers(auto_scale=True)
        client = self.jiffy_client()
        try:
            kv = client.create_hash_table("/a/file.txt", "local://tmp")
            for i in range(0, 2000):
                try:
                    kv.put(b(str(i)), b(str(i)))
                except KeyError as k:
                    self.fail("Received error message: {}".format(k))

            self.assertEqual(2, len(client.fs.dstatus("/a/file.txt").data_blocks))
            for i in range(0, 2000):
                self.assertEqual(b("!ok"), kv.remove(b(str(i))))
            self.assertEqual(1, len(client.fs.dstatus("/a/file.txt").data_blocks))
        finally:
            client.disconnect()
            self.stop_servers()

    def test_file_auto_scale(self):
        self.start_servers(auto_scale=True)
        client = self.jiffy_client()
        try:
            file_client = client.create_file("/a/file.txt", "local://tmp")
            for i in range(0, 4000):
                try:
                    file_client.write(b(str(i)))
                except KeyError as k:
                    self.fail("Received error message: {}".format(k))

            self.assertEqual(2, len(client.fs.dstatus("/a/file.txt").data_blocks))
            self.assertTrue(file_client.seek(0))
            for i in range(0, 4000):
                self.assertEqual(b(str(i)), file_client.read(len(b(str(i)))))
            self.assertEqual(2, len(client.fs.dstatus("/a/file.txt").data_blocks))
        finally:
            client.disconnect()
            self.stop_servers()

    def test_file_asyn_read_write(self):
        self.start_servers(auto_scale=True)
        client = self.jiffy_client()
        data_size = 25000
        data = b("")
        for i in range(0, data_size):
            data += b("a")
        file_client = client.create_file("/a/file.txt", "local://tmp")
        self.assertEqual(data_size, file_client.write(data))
        self.assertEqual(4, len(client.fs.dstatus("/a/file.txt").data_blocks))
        self.assertTrue(file_client.seek(0))
        self.assertEqual(data, file_client.read(len(data)))
        self.assertEqual(4, len(client.fs.dstatus("/a/file.txt").data_blocks))
        client.disconnect()
        self.stop_servers()

    def test_fifo_queue_auto_scale(self):
        self.start_servers(auto_scale=True)
        client = self.jiffy_client()
        try:
            queue_client = client.create_queue("/a/file.txt", "local://tmp")
            for i in range(0, 1000):
                try:
                    queue_client.put(b(str(i)))
                except KeyError as k:
                    self.fail("Received error message: {}".format(k))
            self.assertEqual(2, len(client.fs.dstatus("/a/file.txt").data_blocks))
            for i in range(0, 1000):
                self.assertEqual(b(str(i)), queue_client.get())
            self.assertEqual(1, len(client.fs.dstatus("/a/file.txt").data_blocks))
        finally:
            client.disconnect()
            self.stop_servers()

    def test_notifications(self):
        self.start_servers()
        client = self.jiffy_client()
        try:
            client.create_hash_table("/a/file.txt", "local://tmp")

            n1 = client.listen("/a/file.txt")
            n2 = client.listen("/a/file.txt")
            n3 = client.listen("/a/file.txt")

            n1.subscribe(['put'])
            n2.subscribe(['put', 'remove'])
            n3.subscribe(['remove'])

            kv = client.open_hash_table("/a/file.txt")
            kv.put(b'key1', b'value1')
            kv.remove(b'key1')

            self.assertEqual(Notification('put', b('key1')), n1.get_notification())
            n2_notifs = [n2.get_notification(), n2.get_notification()]
            self.assertTrue(Notification('put', b('key1')) in n2_notifs)
            self.assertTrue(Notification('remove', b('key1')) in n2_notifs)
            self.assertEqual(Notification('remove', b('key1')), n3.get_notification())

            with self.assertRaises(queue.Empty):
                n1.get_notification(block=False)
            with self.assertRaises(queue.Empty):
                n2.get_notification(block=False)
            with self.assertRaises(queue.Empty):
                n3.get_notification(block=False)

            n1.unsubscribe(['put'])
            n2.unsubscribe(['remove'])

            kv.put(b'key1', b'value1')
            kv.remove(b'key1')

            self.assertEqual(Notification('put', b'key1'), n2.get_notification())
            self.assertEqual(Notification('remove', b'key1'), n3.get_notification())

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

    def test_large_data_read(self):
        self.start_servers()
        client = self.jiffy_client()
        try:
            c = client.create_file('/a/file.txt', 'local://tmp')
            self.assertTrue(client.fs.exists('/a/file.txt'))
            data = b("")
            data_size = 1024 * 1024
            for i in range(0, data_size):
                data += b("a")
            for i in range(0, 50):
                try:
                    c.write(data)
                except KeyError as k:
                    self.fail('Received error message: {}'.format(k))
            self.assertTrue(c.seek(0))
            for i in range(0, 50):
                self.assertEqual(data, c.read(len(data)))
        finally:
            client.disconnect()
            self.stop_servers()

    def test_queue_multithread(self):
        self.start_servers()
        client = self.jiffy_client()
        threads = list()
        workers = 3
        ops = 1000
        try:
            client.create_queue('/a/file.txt', 'local://tmp')
            self.assertTrue(client.fs.exists('/a/file.txt'))
            result_queue = queue.Queue()
            for index in range(workers):
                x = threading.Thread(target=self.queue_worker, args=(client, QueryType.PUT_QUERY, result_queue, ops))
                threads.append(x)
                x.start()
            for index in range(workers):
                x = threading.Thread(target=self.queue_worker, args=(client, QueryType.GET_QUERY, result_queue, ops))
                threads.append(x)
                x.start()
            for index, thread in enumerate(threads):
                thread.join()
            self.assertEqual(workers * ops, result_queue.qsize())
        finally:
            client.disconnect()
            self.stop_servers()

