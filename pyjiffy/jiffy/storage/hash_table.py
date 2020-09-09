import math
import time
import copy
from bisect import bisect_right

from jiffy.directory.directory_client import ReplicaChain
from jiffy.directory.ttypes import rpc_storage_mode
from jiffy.storage import crc
from jiffy.storage.command import CommandType
from jiffy.storage.compat import b, unicode, bytes, long, basestring, bytes_to_str
from jiffy.storage.data_structure_client import DataStructureClient
from jiffy.storage.data_structure_cache import HashTableCache
from jiffy.storage.replica_chain_client import ReplicaChainClient
from jiffy.storage.subscriber import SubscriptionClient


class HashTableOps:
    exists = b('exists')
    get = b('get')
    put = b('put')
    remove = b('remove')
    update = b('update')
    upsert = b('upsert')
    exists_ls = b('exists_ls')
    get_ls = b('get_ls')
    put_ls = b('put_ls')
    remove_ls = b('remove_ls')
    update_ls = b('update_ls')
    upsert_ls = b('upsert_ls')
    dir_op = b('dir_op')

    op_types = {exists: CommandType.accessor,
                get: CommandType.accessor,
                put: CommandType.mutator,
                remove: CommandType.mutator,
                update: CommandType.accessor,
                upsert: CommandType.mutator,
                exists_ls: CommandType.accessor,
                get_ls: CommandType.accessor,
                put_ls: CommandType.mutator,
                remove_ls: CommandType.mutator,
                update_ls: CommandType.accessor,
                upsert_ls: CommandType.mutator,
                dir_op: CommandType.mutator}


def encode(value):
    if isinstance(value, bytes):
        return value
    elif isinstance(value, (int, long)):
        value = b(str(value))
    elif isinstance(value, float):
        value = b(repr(value))
    elif not isinstance(value, basestring):
        value = unicode(value)
    if isinstance(value, unicode):
        value = value.encode()
    return value


class RedoError(Exception):
    pass


class RedirectError(Exception):
    def __init__(self, message, blocks):
        super(RedirectError, self).__init__(message)
        self.blocks = blocks


class Ht_Cache_Mailbox:
    def __init__(self, ht_client):
        self.ht_client = ht_client
        self.client_id = self.ht_client.blocks[0].seq.client_id

    def __call__(self, notification):
        print(notification)
        if notification.op == "dir_op":
            self.handle_dir_op(notification)
        elif notification.op == "data_trans":
            self.handle_data_trans(notification)

    def handle_dir_op(self, notificaiton):
        recv_msg = notificaiton.data.decode()
        requestor_id = recv_msg[:recv_msg.index(",")]
        recv_msg = recv_msg.lstrip(requestor_id+",")
        original_state = recv_msg[:recv_msg.index(",")]
        recv_msg = recv_msg.lstrip(original_state+",")
        target_state = recv_msg[:recv_msg.index(",")]
        recv_msg = recv_msg.lstrip(target_state+",")
        key = recv_msg[:]
        if requestor_id == str(self.client_id):
            return
        else:
            print(requestor_id,original_state,target_state)
            if original_state == "I" and target_state == "S" and self.ht_client.cache.get_state(key) == "M":
                self.ht_client.cache.change_state(key,"S")
            elif original_state in "IS" and target_state == "M" and self.ht_client.cache.get_state(key) in "MS":
                self.ht_client.cache.change_state(key,"I")
            # send_msg = requestor_id + "," "key=" + key + "," + "value=" + self.ht_client.cache.get(key)
            # self.ht_client.data_trans(send_msg)

    # def handle_data_trans(self, notification):
    #     recv_msg = notification.data
    #     requestor_id = recv_msg[:recv_msg.index(",")]
    #     recv_msg = recv_msg.lstrip(requestor_id+",")
    #     key = recv_msg[recv_msg.index("key=")+4:recv_msg.index("value=")]
    #     recv_msg = recv_msg.lstrip(key+",")
    #     value = recv_msg[recv_msg.index("value=")+6:]

        
        


class HashTable(DataStructureClient):
    def __init__(self, fs, path, block_info, timeout_ms, cache_size=32000000):
        super(HashTable, self).__init__(fs, path, block_info, HashTableOps.op_types, timeout_ms)
        self.slots = [int(chain.name.split('_')[0]) for chain in self.block_info.data_blocks]
        self.cache = HashTableCache(cache_size) 
        self.client_id = self.blocks[0].seq.client_id
        self.cc_subscriber = SubscriptionClient(fs.dstatus(path), callback=Ht_Cache_Mailbox(self)) #cache coherence subscriber
        self.cc_subscriber.subscribe(["dir_op","put","update"])

    def _refresh(self):
        super(HashTable, self)._refresh()
        self.slots = [int(chain.name.split('_')[0]) for chain in self.block_info.data_blocks]

    def _handle_redirect(self, args, response):
        while b(response[0]) == b('!exporting'):
            args_copy = copy.deepcopy(args)
            if args[0] == b("update") or args[0] == b("upsert"):
                args_copy += [response[2], response[3]]
            block_ids = [bytes_to_str(x) for x in response[1].split(b('!'))]
            chain = ReplicaChain(block_ids, 0, 0, rpc_storage_mode.rpc_in_memory)
            while True:
                response = ReplicaChainClient(self.fs, self.path, self.client_cache, chain,
                                            HashTableOps.op_types).run_command_redirected(args_copy)
                if b(response[0]) != b("!redo"):
                    break
        if b(response[0]) == b('!block_moved'):
            self._refresh()
            return None
        if b(response[0]) == b('!full'):
            time.sleep(0.001 * math.pow(2, self.redo_times))
            self.redo_times += 1
            return None
        if b(response[0]) == b("!redo"):
            return None
        return response

    def put(self, key, value):
        if self._run_repeated([HashTableOps.put, key, value])[0] == b'!ok':
            original_state = self.cache.get_state(key)
            inserted_block, evicted_blocks = self.cache.miss_handling([key,value], "M")
            self.dir_op(inserted_block, original_state, "M")
            for evicted_block in evicted_blocks:
                self.dir_op(evicted_block, evicted_block.state, "I")

    def get(self, key):
        if self.cache.exists(key):
            self.cache.hit_handling(key)
            return self.cache.get(key)
        else:
            value = self._run_repeated([HashTableOps.get, key])[1]
            original_state = self.cache.get_state(key)
            inserted_block, evicted_blocks = self.cache.miss_handling([key,value], "S")
            self.dir_op(inserted_block, original_state, "S")
            for evicted_block in evicted_blocks:
                self.dir_op(evicted_block, evicted_block.state, "I")
            return value

    def exists(self, key):
        try:
            if self.cache.exists(key):
                return True
            self._run_repeated([HashTableOps.exists, key])[0]
        except:
            return False
        return True

    def update(self, key, value):
        resp = self._run_repeated([HashTableOps.update, key, value])
        if resp[0] == b'!ok':
            original_state = self.cache.get_state(key)
            inserted_block, evicted_blocks = self.cache.miss_handling([key,value], "M")
            self.dir_op(inserted_block, original_state, "M")
            for evicted_block in evicted_blocks:
                self.dir_op(evicted_block, evicted_block.state, "I")
        return resp[0]

    def upsert(self, key, value):
        if self._run_repeated([HashTableOps.upsert, key, value])[0] == b'!ok':
            original_state = self.cache.get_state(key)
            inserted_block, evicted_blocks = self.cache.miss_handling([key,value], "M")
            self.dir_op(inserted_block,original_state, "M")
            for evicted_block in evicted_blocks:
                self.dir_op(evicted_block, evicted_block.state, "I")

    def remove(self, key):
        resp = self._run_repeated([HashTableOps.remove, key])
        if resp[0] == b'!ok':
            self.cache.delete(key)
        return resp[0]

    def put_ls(self, key, value):
        if self._run_repeated([HashTableOps.put_ls, key, value])[0] == b'!ok':
            original_state = self.cache.get_state(key)
            inserted_block, evicted_blocks = self.cache.miss_handling([key,value], "M")
            self.dir_op(inserted_block, original_state, "M")
            for evicted_block in evicted_blocks:
                self.dir_op(evicted_block, evicted_block.state, "I")

    def get_ls(self, key):
        if self.cache.exists(key):
            self.cache.hit_handling(key)
            return self.cache.get(key)
        else:
            value = self._run_repeated([HashTableOps.get_ls, key])[1]
            original_state = self.cache.get_state(key)
            inserted_block, evicted_blocks = self.cache.miss_handling([key,value], "S")
            self.dir_op(inserted_block, original_state, "S")
            for evicted_block in evicted_blocks:
                self.dir_op(evicted_block, evicted_block.state, "I")
            return value

    def exists_ls(self, key):
        try:
            if self.cache.exists(key):
                return True
            self._run_repeated([HashTableOps.exists_ls, key])[0]
        except:
            return False
        return True

    def update_ls(self, key, value):
        resp = self._run_repeated([HashTableOps.update_ls, key, value])
        if resp[0] == b'!ok':
            original_state = self.cache.get_state(key)
            inserted_block, evicted_blocks = self.cache.miss_handling([key,value], "M")
            self.dir_op(inserted_block, original_state, "M")
            for evicted_block in evicted_blocks:
                self.dir_op(evicted_block, evicted_block.state, "I")
        return resp[0]

    def upsert_ls(self, key, value):
        if self._run_repeated([HashTableOps.upsert_ls, key, value])[0] == b'!ok':
            original_state = self.cache.get_state(key)
            inserted_block, evicted_blocks = self.cache.miss_handling([key,value], "M")
            self.dir_op(inserted_block, original_state, "M")
            for evicted_block in evicted_blocks:
                self.dir_op(evicted_block, evicted_block.state, "I")

    def remove_ls(self, key):
        resp = self._run_repeated([HashTableOps.remove_ls, key])
        if resp[0] == b'!ok':
            self.cache.delete(key)
        return resp[0]

    def dir_op(self, cache_entry, original_state, target_state):
        msg = ""
        msg = str(self.client_id) + "," + original_state + "," + target_state + "," + str(cache_entry.mark)
        self._run_repeated([HashTableOps.dir_op, msg])
        
    # def data_trans(self, msg):
    #     self._run_repeated([HashTableOps.data_trans, msg])

    def _block_id(self, args):
        i = bisect_right(self.slots, crc.crc16(encode(args[1])))
        if i:
            return i - 1
        raise ValueError
