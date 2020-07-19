class Node(object):
    def __init__(self, mark, data): # Only file cache will use param 'offset'
        self.mark = mark
        self.data = data
        self.next = None
        self.prev = None

class LinkedList(object):
    def __init__(self, max_length, max_size):
        self.head = None
        self.tail = None
        self.length = 0
        self.size = 0
        self.max_length = max_length
        self.max_size = max_size

    def length_(self):
        return self.length
    
    def size_(self):
        return self.size 

    def max_length_(self):
        return self.max_length

    def max_size_(self):
        return self.max_size

    def insert_head(self, node):
        if self.head != None:
            node.next = self.head
            self.head.prev = node
            self.head = node
        else:
            self.head = node
            self.tail = node
        self.length += 1
        self.size += len(node.data)
    
    def move_to_head(self, node):
        if node == self.head:
            return
        elif node == self.tail:
            node.prev.next = node.next
            self.tail = node.prev
            node.next = self.head
            self.head.prev = node
            node.prev = None
            self.head = node
        else:
            node.prev.next = node.next
            node.next.prev = node.prev
            node.next = self.head
            self.head.prev = node
            node.prev = None
            self.head = node
    
    def pop(self):
        if self.length == 0:
            return 
        elif self.length == 1:
            self.head = None
            self.tail = None
            self.length = 0
            self.size = 0
        else:
            self.length -= 1
            self.size -= len(self.tail.data)
            self.tail.prev.next = None
            self.tail = self.tail.prev
    
    def remove(self, node):
        if self.head == self.tail:
            self.head = None
            self.tail = None
        elif node == self.head:
            self.head = node.next
            self.head.prev = None
        elif node == self.tail:
            node.prev.next = None
            self.tail = node.prev
        else:
            node.prev.next = node.next
            node.next.prev = node.prev
        node.prev = None
        node.next = None
        self.length -= 1
        self.size -= len(node.data)


class DataStructureCache(object):
    def __init__(self, max_length, max_size):
        self.list = LinkedList(max_length, max_size)

    def isEmpty(self):
        return self.list.length_() == 0 or self.list.size_() == 0

    def isFull(self):
        return self.list.length_() >= self.list.max_length_() or self.list.size_() >= self.list.max_size_()

    def length(self):
        return self.list.length_()
    
    def size(self):
        return self.list.size_()    


class HashTableCache(DataStructureCache):
    def __init__(self,max_size): 
        super(HashTableCache, self).__init__(float('inf'), max_size)
        self.table = dict()
    
    def insert(self, kv):
        new_node = Node(kv[0], kv[1])
        self.table[kv[0]] = new_node
        self.list.insert_head(new_node)
    
    def delete(self, key):
        if key not in self.table:
            return
        self.list.remove(self.table[key])
        del self.table[key]

    def evict(self):
        del self.table[self.list.tail.mark]
        self.list.pop()
    
    def exists(self, key):
        return key in self.table

    def get(self, key):
        return self.table[key].data

    def update(self, key, new_value):
        self.table[key].data = new_value

    def hit_handling(self, key):
        self.list.move_to_head(self.table[key])

    def miss_handling(self, raw_data):
        new_value = raw_data[1]
        new_key = raw_data[0]
        if self.exists(new_key):
            if self.get(new_key) == new_value:
                self.hit_handling(new_key)
            else:
                old_value = self.get(new_key)
                while (self.size() + len(new_value)- len(old_value)) > self.list.max_size_():
                    self.evict()
                self.update(new_key,new_value)
                self.hit_handling(new_key)
        else:
            while (self.size() + len(new_value)) > self.list.max_size_():
                self.evict()
            self.insert([new_key,new_value])
        
    
    def print_out(self): # for debug use
        cur_node = self.list.head
        while cur_node:
            print(cur_node.mark,self.table[cur_node.mark])
            cur_node = cur_node.next


class FileCache(DataStructureCache):
    def __init__(self, max_length, block_size, prefetch_block_num):
        super(FileCache,self).__init__(max_length, float('inf'))
        self.block_size = block_size
        self.prefetch_block_num = prefetch_block_num
        self.table = dict()
    
    def insert(self, offset, data):
        new_node = Node(offset, data)
        self.table[offset] = new_node
        self.list.insert_head(new_node)

    def evict(self):
        del self.table[self.list.tail.mark]
        self.list.pop()
    
    def exists(self, cur_offset):
        start_offset = (cur_offset // self.block_size) * self.block_size
        if start_offset in self.table and (cur_offset - start_offset) <= len(self.table[start_offset].data):
            return True
        return False

    def prefetch_handling(self, start_offset, prefetch_data):
        block_list = []
        offset_list = []
        offset = start_offset
        end_offset = start_offset + (len(prefetch_data) - 1) // self.block_size * self.block_size
        while offset <= end_offset:
            offset_list.append(offset)
            block_list.append(prefetch_data[(offset - start_offset):(offset - start_offset + min(self.block_size, len(prefetch_data) - (offset - start_offset)))])
            offset += self.block_size
        for i in range(len(offset_list)-1,-1,-1):
            self.miss_handling(offset_list[i], block_list[i])

    def miss_handling(self, cur_offset, data):
        start_offset = (cur_offset // self.block_size) * self.block_size
        if start_offset in self.table:
            self.table[start_offset].data = data
            self.list.move_to_head(self.table[start_offset])
        else:
            if self.isFull():
                self.evict()
            self.insert(start_offset, data)
        
    def hit_handling(self,cur_offset, read_size):
        start_offset = (cur_offset // self.block_size) * self.block_size
        return self.table[start_offset].data[(cur_offset - start_offset) : min(read_size + cur_offset - start_offset, len(self.table[start_offset].data))]
        
    def print_out(self): # for debug use
        cur_node = self.list.head
        while cur_node:
            print(cur_node.mark,cur_node.data)
            cur_node = cur_node.next
        
    
    
