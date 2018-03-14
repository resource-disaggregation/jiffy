import os
import time
import subprocess
from unittest import TestCase

from thrift.transport import TTransport

from elasticmem.client import ElasticMemClient
from elasticmem.directory import directory_client
from elasticmem.directory import lease_client
from elasticmem.kv import kv_client
from elasticmem.kv.ttypes import kv_rpc_exception


class TestClient(TestCase):
    DIRECTORY_SERVER_EXECUTABLE = os.getenv('DIRECTORY_SERVER_EXEC', 'directoryd')
    KV_SERVER_EXECUTABLE = os.getenv('KV_SERVER_EXEC', 'kvd')
    KV_HOST = '127.0.0.1'
    KV_PORT = 9092
    DIRECTORY_HOST = '127.0.0.1'
    DIRECTORY_SERVICE_PORT = 9090
    DIRECTORY_LEASE_PORT = 9091

    def wait_till_servers_ready(self):
        check = True
        while check:
            try:
                directory_client.DirectoryClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT)
                lease_client.LeaseClient(self.DIRECTORY_HOST, self.DIRECTORY_LEASE_PORT)
                kv_client.BlockConnection("%s:%d:0" % (self.KV_HOST, self.KV_PORT))
                check = False
            except TTransport.TTransportException:
                time.sleep(0.1)

    def start_servers(self):
        print "Starting directory: %s" % self.DIRECTORY_SERVER_EXECUTABLE
        self.directoryd = subprocess.Popen([self.DIRECTORY_SERVER_EXECUTABLE])
        print "Starting kv: %s" % self.KV_SERVER_EXECUTABLE
        self.kvd = subprocess.Popen([self.KV_SERVER_EXECUTABLE])
        self.wait_till_servers_ready()

    def stop_servers(self):
        self.directoryd.kill()
        self.directoryd.wait()
        self.kvd.kill()
        self.kvd.wait()

    def kv_ops(self, kv):
        # Test get/put
        for i in range(0, 1000):
            kv.put(str(i), str(i))

        for i in range(0, 1000):
            self.assertTrue(kv.get(str(i)) == str(i))

        for i in range(1000, 2000):
            with self.assertRaises(kv_rpc_exception):
                kv.get(str(i))

        # Test update
        for i in range(0, 1000):
            kv.update(str(i), str(i + 1000))

        for i in range(1000, 2000):
            with self.assertRaises(kv_rpc_exception):
                kv.update(str(i), str(i))

        for i in range(0, 1000):
            self.assertTrue(kv.get(str(i)) == str(i + 1000))

        # Test remove
        for i in range(0, 1000):
            kv.remove(str(i))

        for i in range(1000, 2000):
            with self.assertRaises(kv_rpc_exception):
                kv.remove(str(i))

        for i in range(0, 1000):
            with self.assertRaises(kv_rpc_exception):
                kv.get(str(i))

    def test_create_scope(self):
        self.start_servers()
        client = ElasticMemClient(self.DIRECTORY_HOST, self.DIRECTORY_SERVICE_PORT, self.DIRECTORY_LEASE_PORT)

        try:
            self.kv_ops(client.create_scope("/a/file.txt", "/tmp"))
        except:
            self.stop_servers()
            raise

        client.close()

    def test_get_scope(self):
        pass

    def test_destroy_scope(self):
        pass
