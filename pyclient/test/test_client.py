import os
import subprocess
import sys
import time
from unittest import TestCase
from Queue import Empty

from thrift.transport import TTransport

from elasticmem import ElasticMemClient, RemoveMode
from elasticmem import BlockException
from elasticmem.directory import directory_client
from elasticmem.kv import kv_client
from elasticmem.lease import lease_client
from elasticmem.subscription import subscriber
from elasticmem.subscription.subscriber import Notification


class TestClient(TestCase):
    DIRECTORY_SERVER_EXECUTABLE = os.getenv('DIRECTORY_SERVER_EXEC', 'directoryd')
    STORAGE_SERVER_EXECUTABLE = os.getenv('STORAGE_SERVER_EXEC', 'storaged')
    KV_HOST = '127.0.0.1'
    KV_PORT = 9093
    NOTIFICATION_PORT = 9095
    DIRECTORY_HOST = '127.0.0.1'
    DIRECTORY_SERVICE_PORT = 9090
    DIRECTORY_LEASE_PORT = 9091

    def wait_till_directory_server_ready(self):
        check = True
        while check:
            try:
                directory_client.DirectoryClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT)
                lease_client.LeaseClient(self.DIRECTORY_HOST, self.DIRECTORY_LEASE_PORT)
                check = False
            except TTransport.TTransportException:
                time.sleep(0.1)

    def wait_till_storage_server_ready(self):
        check = True
        while check:
            try:
                kv_client.BlockConnection("%s:%d:0" % (self.KV_HOST, self.KV_PORT))
                subscriber.SubscriptionClient(["%s:%d:0" % (self.KV_HOST, self.NOTIFICATION_PORT)])
                check = False
            except TTransport.TTransportException:
                time.sleep(0.1)

    def start_servers(self):
        try:
            self.directoryd = subprocess.Popen([self.DIRECTORY_SERVER_EXECUTABLE])
        except OSError as e:
            print "Error running executable %s: %s" % (self.DIRECTORY_SERVER_EXECUTABLE, e)
            sys.exit()

        self.wait_till_directory_server_ready()

        try:
            self.storaged = subprocess.Popen([self.STORAGE_SERVER_EXECUTABLE])
        except OSError as e:
            print "Error running executable %s: %s" % (self.STORAGE_SERVER_EXECUTABLE, e)
            sys.exit()

        self.wait_till_storage_server_ready()

    def stop_servers(self):
        self.directoryd.kill()
        self.directoryd.wait()
        self.storaged.kill()
        self.storaged.wait()

    def kv_ops(self, kv):
        # Test get/put
        for i in range(0, 1000):
            kv.put(str(i), str(i))

        for i in range(0, 1000):
            self.assertTrue(kv.get(str(i)) == str(i))

        for i in range(1000, 2000):
            with self.assertRaises(BlockException):
                kv.get(str(i))

        # Test update
        for i in range(0, 1000):
            kv.update(str(i), str(i + 1000))

        for i in range(1000, 2000):
            with self.assertRaises(BlockException):
                kv.update(str(i), str(i))

        for i in range(0, 1000):
            self.assertTrue(kv.get(str(i)) == str(i + 1000))

        # Test remove
        for i in range(0, 1000):
            kv.remove(str(i))

        for i in range(1000, 2000):
            with self.assertRaises(BlockException):
                kv.remove(str(i))

        for i in range(0, 1000):
            with self.assertRaises(BlockException):
                kv.get(str(i))

    def test_lease_worker(self):
        self.start_servers()
        client = ElasticMemClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT)
        try:
            client.create_scope("/a/file.txt", "/tmp")
            self.assertTrue(client.fs.exists("/a/file.txt"))
            time.sleep(client.lease_worker.renewal_duration_s)
            self.assertTrue(client.fs.exists("/a/file.txt"))
            time.sleep(client.lease_worker.renewal_duration_s)
            self.assertTrue(client.fs.exists("/a/file.txt"))
        except:
            self.stop_servers()
            client.close()
            raise

        client.close()
        self.stop_servers()

    def test_create_scope(self):
        self.start_servers()
        client = ElasticMemClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT)
        try:
            self.kv_ops(client.create_scope("/a/file.txt", "/tmp"))
            self.assertTrue(client.fs.exists('/a/file.txt'))
        except:
            self.stop_servers()
            client.close()
            raise

        client.close()
        self.stop_servers()

    def test_get_scope(self):
        self.start_servers()
        client = ElasticMemClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT)
        try:
            client.create_scope("/a/file.txt", "/tmp")
            self.assertTrue(client.fs.exists('/a/file.txt'))
            self.kv_ops(client.get_scope('/a/file.txt'))
        except:
            self.stop_servers()
            client.close()
            raise

        client.close()
        self.stop_servers()

    def test_destroy_scope(self):
        self.start_servers()
        client = ElasticMemClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT)
        try:
            client.create_scope("/a/file.txt", "/tmp")
            self.assertTrue('/a/file.txt' in client.to_renew)
            client.destroy_scope('/a/file.txt', RemoveMode.flush)
            self.assertFalse('/a/file.txt' in client.to_renew)
            self.assertTrue('/a/file.txt' in client.to_flush)
            client.destroy_scope('/a/file.txt', RemoveMode.delete)
            self.assertFalse('/a/file.txt' in client.to_renew)
            self.assertTrue('/a/file.txt' in client.to_remove)
        except:
            self.stop_servers()
            client.close()
            raise

        client.close()
        self.stop_servers()

    def test_notifications(self):
        self.start_servers()
        client = ElasticMemClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT)
        try:
            client.fs.create_file("/a/file.txt", "/tmp")

            n1 = client.notifications("/a/file.txt")
            n2 = client.notifications("/a/file.txt")
            n3 = client.notifications("/a/file.txt")

            n1.subscribe(['put'])
            n2.subscribe(['put', 'remove'])
            n3.subscribe(['remove'])

            kv = client.get_scope("/a/file.txt")
            kv.put('key1', 'value1')
            kv.remove('key1')

            self.assertTrue(n1.get_notification() == Notification('put', 'key1'))
            self.assertTrue(n2.get_notification() == Notification('put', 'key1'))
            self.assertTrue(n2.get_notification() == Notification('remove', 'key1'))
            self.assertTrue(n3.get_notification() == Notification('remove', 'key1'))

            with self.assertRaises(Empty):
                n1.get_notification(block=False)
            with self.assertRaises(Empty):
                n2.get_notification(block=False)
            with self.assertRaises(Empty):
                n3.get_notification(block=False)

            n1.unsubscribe(['put'])
            n2.unsubscribe(['remove'])

            kv.put('key1', 'value1')
            kv.remove('key1')

            self.assertTrue(n2.get_notification() == Notification('put', 'key1'))
            self.assertTrue(n3.get_notification() == Notification('remove', 'key1'))

            with self.assertRaises(Empty):
                n1.get_notification(block=False)
            with self.assertRaises(Empty):
                n2.get_notification(block=False)
            with self.assertRaises(Empty):
                n3.get_notification(block=False)

            n1.close()
            n2.close()
            n3.close()
        except:
            self.stop_servers()
            client.close()
            raise

        client.close()
        self.stop_servers()
