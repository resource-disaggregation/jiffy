import argparse
from jiffy import JiffyClient, b, Flags
from jiffy.benchmark.zipf_generator import gen_zipf
import time
import threading

class FileBenchmark:
    def __init__(self, clients, data_size, num_clients, num_ops):
        self.data_ = "x" * data_size
        self.num_clients = num_clients
        self.num_ops_ = int(num_ops / num_clients)
        self.clients_ = clients
        self.workers_ = [None] * self.num_clients
        self.total_bytes_ = [None] * self.num_clients
        self.cache_hit_ = [None] * self.num_clients
        self.total_access_ = [None] * self.num_clients
        self.throughput_ = [None] * self.num_clients
        self.latency_write_ = [None] * self.num_clients
        self.latency_read_ = [None] * self.num_clients

    def wait(self):
        throughput = 0.0
        latency_read = 0.0
        latency_write = 0.0
        hit = 0
        access = 0
        for i in range(self.num_clients):
            self.workers_[i].join()
            throughput += self.throughput_[i]
            latency_read += self.latency_read_[i]
            latency_write += self.latency_write_[i]
            hit += self.cache_hit_[i]
            access += self.total_access_[i]
        return [throughput, latency_write / self.num_clients, latency_read / self.num_clients, float(hit * 100 / access)]

class WriteBenchmark(FileBenchmark):
    def __init__(self, clients, data_size, num_clients, num_ops):
        super(WriteBenchmark, self).__init__(clients, data_size, num_clients, num_ops)

    def run(self):
        for i in range(self.num_clients):
            self.workers_[i] = threading.Thread(target = self.single_thread_action, args = (i,))
        for i in range(self.num_clients):
            self.workers_[i].start()
                
    def single_thread_action(self, thread_index):
        bench_begin = time.time()
        tot_time = 0.0
        t0, t1 = bench_begin, bench_begin
        for j in range(self.num_ops_):
            t0 = time.time()
            self.clients_[thread_index].write(self.data_)
            t1 = time.time()
            tot_time += (t1 - t0)
        j += 1
        self.latency_[thread_index] = (10 ** 6) * float(tot_time) / float(j)
        self.throughput_[thread_index] = j / (t1 - bench_begin)


class ReadBenchmark(FileBenchmark):
    def __init__(self, clients, data_size, num_clients, num_ops):
        super(ReadBenchmark, self).__init__(clients, data_size, num_clients, num_ops)
        self.reading_offsets = gen_zipf(1, len(self.data_) * num_ops, num_ops)

    def run(self):
        for i in range(self.num_clients):
            self.workers_[i] = threading.Thread(target = self.single_thread_action, args = (i,))
        for i in range(self.num_clients):
            self.workers_[i].start()
                
    def single_thread_action(self, thread_index):
        cache_hit = 0
        total_access = 0
        total_bytes = 0
        write_begin = time.time()
        write_time = 0.0
        t0, t1 = write_begin, write_begin
        for j in range(self.num_ops_):
            t0 = time.time()
            self.clients_[thread_index].write(self.data_)
            t1 = time.time()
            write_time += (t1 - t0)
        self.clients_[thread_index].seek(0)
        read_begin = time.time()
        read_time = 0.0
        t0, t1 = read_begin, read_begin
        for j in range(self.num_ops_):
            self.clients_[thread_index].seek(self.reading_offsets[j])
            t0 = time.time()
            resp = self.clients_[thread_index].read(len(self.data_))
            t1 = time.time()
            read_time += (t1 - t0)
            total_access += resp[1]
            cache_hit += resp[2]
            total_bytes += len(resp[0])
            
        self.total_access_[thread_index] = total_access
        self.cache_hit_[thread_index] = cache_hit 
        self.total_bytes_[thread_index] = total_bytes
        self.latency_write_[thread_index] = (10 ** 6) * float(write_time) / float(self.num_ops_)
        self.latency_read_[thread_index] = (10 ** 6) * float(read_time) / float(self.num_ops_)
        self.throughput_[thread_index] = total_bytes / (t1 - read_begin)


def file_bp_zipf():
    address = "frog.zoo.cs.yale.edu"
    service_port = 9090
    lease_port = 9091
    num_blocks = 1
    chain_length = 1
    num_ops = 100000
    data_size = 64
    op_type_set = []
    op_type_set.append("write")
    op_type_set.append("read")
    path = "/tmp"
    backing_path = "local://tmp"

    # Output all the configuration parameters:
    file_name = './file_bp_zipf.txt'
    data = open(file_name,'w+') 
    print >> data, "host: ", address
    print >> data, "service-port: ", service_port
    print >> data, "lease-port: ", lease_port
    print >> data, "num-blocks: ", num_blocks
    print >> data, "chain-length: ", chain_length
    print >> data, "num-ops: ", num_ops
    print >> data, "data-size: ", data_size
    print >> data, "path: ", path
    print >> data, "backing-path: ", backing_path
    for op_type in op_type_set:
        count = 1
        while count <= 1:
            loading = 0
            num_clients = count
            cache_size = 200
            client = JiffyClient(address, service_port, lease_port)
            ht_clients = [None] * num_clients
            for cache_block_size in range(1000, 32001, 1000): 
                for prefetch_size in range(5, 51, 5):
                    for i in range(num_clients): 
                        ht_clients[i] = client.open_or_create_file(path, backing_path, num_blocks, chain_length, cache_size, cache_block_size, prefetch_size)
                    
                    if (op_type == "write"):
                    	benchmark = WriteBenchmark(ht_clients, data_size, num_clients, num_ops)
                    if (op_type == "read"):
                        benchmark = ReadBenchmark(ht_clients, data_size, num_clients, num_ops)
                    else:
                        print >> data, "Incorrect operation type for file: ", op_type
                        return 0
                    
                    benchmark.run()
                    result = benchmark.wait()
                    client.remove(path)
                    
                    print >> data, "===== ", "Block Size= ", cache_block_size, "Prefetch Size= ", prefetch_size, " ======"
                    print >> data, "\t", num_ops, " requests completed in ", (float(num_ops) / result[0]), " s"
                    print >> data, "\t", num_clients, " parallel clients"
                    print >> data, "\t", data_size, " payload"
                    print >> data, "\tAverage write latency: ", result[1], "us"
                    print >> data, "\tAverage read latency: ", result[2], "us"
                    print >> data, "\tAverage total latency: ", result[1]+result[2], "us"
                    print >> data, "\tThroughput: ", result[0], " bytes per second"
                    print >> data, "\tHit_rate: ", round(result[3],4), "%"
                    print >> data, "\n"
                    loading += 1
                    print("Loading -- ", round(float(loading*100/320),1), "%")
            count *= 2

    data.close()
    return 0

def file_bp():
    address = "frog.zoo.cs.yale.edu"
    service_port = 9090
    lease_port = 9091
    num_blocks = 1
    chain_length = 1
    num_ops = 100000
    data_size = 64
    op_type_set = []
    op_type_set.append("write")
    op_type_set.append("read")
    path = "/tmp"
    backing_path = "local://tmp"

    # Output all the configuration parameters:
    file_name = './file_bp.txt'
    data = open(file_name,'w+') 
    print >> data, "host: ", address
    print >> data, "service-port: ", service_port
    print >> data, "lease-port: ", lease_port
    print >> data, "num-blocks: ", num_blocks
    print >> data, "chain-length: ", chain_length
    print >> data, "num-ops: ", num_ops
    print >> data, "data-size: ", data_size
    print >> data, "path: ", path
    print >> data, "backing-path: ", backing_path
    for op_type in op_type_set:
        count = 1
        while count <= 1:
            loading = 0
            num_clients = count
            cache_size = 200
            client = JiffyClient(address, service_port, lease_port)
            ht_clients = [None] * num_clients
            for cache_block_size in range(1000, 32001, 1000): 
                for prefetch_size in range(5, 51, 5):
                    for i in range(num_clients): 
                        ht_clients[i] = client.open_or_create_file(path, backing_path, num_blocks, chain_length, cache_size, cache_block_size, prefetch_size)
                    
                    if (op_type == "write"):
                    	benchmark = WriteBenchmark(ht_clients, data_size, num_clients, num_ops)
                    if (op_type == "read"):
                        benchmark = ReadBenchmark(ht_clients, data_size, num_clients, num_ops)
                    else:
                        print >> data, "Incorrect operation type for file: ", op_type
                        return 0
                    
                    benchmark.run()
                    result = benchmark.wait()
                    client.remove(path)
                    
                    print >> data, "===== ", "Block Size= ", cache_block_size, "Prefetch Size= ", prefetch_size, " ======"
                    print >> data, "\t", num_ops, " requests completed in ", (float(num_ops) / result[0]), " s"
                    print >> data, "\t", num_clients, " parallel clients"
                    print >> data, "\t", data_size, " payload"
                    print >> data, "\tAverage write latency: ", result[1], "us"
                    print >> data, "\tAverage read latency: ", result[2], "us"
                    print >> data, "\tAverage total latency: ", result[1]+result[2], "us"
                    print >> data, "\tThroughput: ", result[0], " bytes per second"
                    print >> data, "\tHit_rate: ", round(result[3],4), "%"
                    print >> data, "\n"
                    loading += 1
                    print("Loading -- ", round(float(loading*100/320),1), "%")
            count *= 2

    data.close()
    return 0

def file_cb_zipf():
    address = "frog.zoo.cs.yale.edu"
    service_port = 9090
    lease_port = 9091
    num_blocks = 1
    chain_length = 1
    num_ops = 100000
    data_size = 64
    op_type_set = []
    op_type_set.append("write")
    op_type_set.append("read")
    path = "/tmp"
    backing_path = "local://tmp"

    # Output all the configuration parameters:
    file_name = './file_cb_zipf.txt'
    data = open(file_name,'w+') 
    print >> data, "host: ", address
    print >> data, "service-port: ", service_port
    print >> data, "lease-port: ", lease_port
    print >> data, "num-blocks: ", num_blocks
    print >> data, "chain-length: ", chain_length
    print >> data, "num-ops: ", num_ops
    print >> data, "data-size: ", data_size
    print >> data, "path: ", path
    print >> data, "backing-path: ", backing_path
    for op_type in op_type_set:
        count = 1
        while count <= 1:
            loading = 0
            num_clients = count
            prefetch_size = 20
            client = JiffyClient(address, service_port, lease_port)
            ht_clients = [None] * num_clients
            for cache_size in range(100, 2101, 200): 
                for cache_block_size in range(1000, 20001, 1000):
                    for i in range(num_clients): 
                        ht_clients[i] = client.open_or_create_file(path, backing_path, num_blocks, chain_length, cache_size, cache_block_size, prefetch_size)
                    
                    if (op_type == "write"):
                    	benchmark = WriteBenchmark(ht_clients, data_size, num_clients, num_ops)
                    if (op_type == "read"):
                        benchmark = ReadBenchmark(ht_clients, data_size, num_clients, num_ops)
                    else:
                        print >> data, "Incorrect operation type for file: ", op_type
                        return 0
                    
                    benchmark.run()
                    result = benchmark.wait()
                    client.remove(path)
                    
                    print >> data, "===== ", "Cache_Size= ", cache_size, "Block Size= ", cache_block_size, " ======"
                    print >> data, "\t", num_ops, " requests completed in ", (float(num_ops) / result[0]), " s"
                    print >> data, "\t", num_clients, " parallel clients"
                    print >> data, "\t", data_size, " payload"
                    print >> data, "\tAverage write latency: ", result[1], "us"
                    print >> data, "\tAverage read latency: ", result[2], "us"
                    print >> data, "\tAverage total latency: ", result[1]+result[2], "us"
                    print >> data, "\tThroughput: ", result[0], " bytes per second"
                    print >> data, "\tHit_rate: ", round(result[3],4), "%"
                    print >> data, "\n"
                    loading += 1
                    print("Loading -- ", round(float(loading*100/220),1), "%")
            count *= 2

    data.close()
    return 0

def file_cb():
    address = "frog.zoo.cs.yale.edu"
    service_port = 9090
    lease_port = 9091
    num_blocks = 1
    chain_length = 1
    num_ops = 100000
    data_size = 64
    op_type_set = []
    op_type_set.append("write")
    op_type_set.append("read")
    path = "/tmp"
    backing_path = "local://tmp"

    # Output all the configuration parameters:
    file_name = './file_cb.txt'
    data = open(file_name,'w+') 
    print >> data, "host: ", address
    print >> data, "service-port: ", service_port
    print >> data, "lease-port: ", lease_port
    print >> data, "num-blocks: ", num_blocks
    print >> data, "chain-length: ", chain_length
    print >> data, "num-ops: ", num_ops
    print >> data, "data-size: ", data_size
    print >> data, "path: ", path
    print >> data, "backing-path: ", backing_path
    for op_type in op_type_set:
        count = 1
        while count <= 1:
            loading = 0
            num_clients = count
            prefetch_size = 20
            client = JiffyClient(address, service_port, lease_port)
            ht_clients = [None] * num_clients
            for cache_size in range(100, 2101, 200): 
                for cache_block_size in range(1000, 20001, 1000):
                    for i in range(num_clients): 
                        ht_clients[i] = client.open_or_create_file(path, backing_path, num_blocks, chain_length, cache_size, cache_block_size, prefetch_size)
                    
                    if (op_type == "write"):
                    	benchmark = WriteBenchmark(ht_clients, data_size, num_clients, num_ops)
                    if (op_type == "read"):
                        benchmark = ReadBenchmark(ht_clients, data_size, num_clients, num_ops)
                    else:
                        print >> data, "Incorrect operation type for file: ", op_type
                        return 0
                    
                    benchmark.run()
                    result = benchmark.wait()
                    client.remove(path)
                    
                    print >> data, "===== ", "Cache_Size= ", cache_size, "Block Size= ", cache_block_size, " ======"
                    print >> data, "\t", num_ops, " requests completed in ", (float(num_ops) / result[0]), " s"
                    print >> data, "\t", num_clients, " parallel clients"
                    print >> data, "\t", data_size, " payload"
                    print >> data, "\tAverage write latency: ", result[1], "us"
                    print >> data, "\tAverage read latency: ", result[2], "us"
                    print >> data, "\tAverage total latency: ", result[1]+result[2], "us"
                    print >> data, "\tThroughput: ", result[0], " bytes per second"
                    print >> data, "\tHit_rate: ", round(result[3],4), "%"
                    print >> data, "\n"
                    loading += 1
                    print("Loading -- ", round(float(loading*100/220),1), "%")
            count *= 2

    data.close()
    return 0

def file_cp_zipf():
    address = "frog.zoo.cs.yale.edu"
    service_port = 9090
    lease_port = 9091
    num_blocks = 1
    chain_length = 1
    num_ops = 100000
    data_size = 64
    op_type_set = []
    op_type_set.append("write")
    op_type_set.append("read")
    path = "/tmp"
    backing_path = "local://tmp"

    # Output all the configuration parameters:
    file_name = './file_cp_zipf.txt'
    data = open(file_name,'w+') 
    print >> data, "host: ", address
    print >> data, "service-port: ", service_port
    print >> data, "lease-port: ", lease_port
    print >> data, "num-blocks: ", num_blocks
    print >> data, "chain-length: ", chain_length
    print >> data, "num-ops: ", num_ops
    print >> data, "data-size: ", data_size
    print >> data, "path: ", path
    print >> data, "backing-path: ", backing_path
    for op_type in op_type_set:
        count = 1
        while count <= 1:
            loading = 0
            num_clients = count
            cache_block_size = 2000
            client = JiffyClient(address, service_port, lease_port)
            ht_clients = [None] * num_clients
            for cache_size in range(100, 2101, 200): 
                for prefetch_size in range(5, 51, 5):
                    for i in range(num_clients): 
                        ht_clients[i] = client.open_or_create_file(path, backing_path, num_blocks, chain_length, cache_size, cache_block_size, prefetch_size)
                    
                    if (op_type == "write"):
                    	benchmark = WriteBenchmark(ht_clients, data_size, num_clients, num_ops)
                    if (op_type == "read"):
                        benchmark = ReadBenchmark(ht_clients, data_size, num_clients, num_ops)
                    else:
                        print >> data, "Incorrect operation type for file: ", op_type
                        return 0
                    
                    benchmark.run()
                    result = benchmark.wait()
                    client.remove(path)
                    
                    print >> data, "===== ", "Cache_Size= ", cache_size, "Prefetch Size= ", prefetch_size, " ======"
                    print >> data, "\t", num_ops, " requests completed in ", (float(num_ops) / result[0]), " s"
                    print >> data, "\t", num_clients, " parallel clients"
                    print >> data, "\t", data_size, " payload"
                    print >> data, "\tAverage write latency: ", result[1], "us"
                    print >> data, "\tAverage read latency: ", result[2], "us"
                    print >> data, "\tAverage total latency: ", result[1]+result[2], "us"
                    print >> data, "\tThroughput: ", result[0], " bytes per second"
                    print >> data, "\tHit_rate: ", round(result[3],4), "%"
                    print >> data, "\n"
                    loading += 1
                    print("Loading -- ", round(float(loading*100/110),1), "%")
            count *= 2

    data.close()
    return 0

def file_cp():
    address = "frog.zoo.cs.yale.edu"
    service_port = 9090
    lease_port = 9091
    num_blocks = 1
    chain_length = 1
    num_ops = 100000
    data_size = 64
    op_type_set = []
    op_type_set.append("write")
    op_type_set.append("read")
    path = "/tmp"
    backing_path = "local://tmp"

    # Output all the configuration parameters:
    file_name = './file_cp.txt'
    data = open(file_name,'w+') 
    print >> data, "host: ", address
    print >> data, "service-port: ", service_port
    print >> data, "lease-port: ", lease_port
    print >> data, "num-blocks: ", num_blocks
    print >> data, "chain-length: ", chain_length
    print >> data, "num-ops: ", num_ops
    print >> data, "data-size: ", data_size
    print >> data, "path: ", path
    print >> data, "backing-path: ", backing_path
    for op_type in op_type_set:
        count = 1
        while count <= 1:
            loading = 0
            num_clients = count
            cache_block_size = 2000
            client = JiffyClient(address, service_port, lease_port)
            ht_clients = [None] * num_clients
            for cache_size in range(100, 2101, 200): 
                for prefetch_size in range(5, 51, 5):
                    for i in range(num_clients): 
                        ht_clients[i] = client.open_or_create_file(path, backing_path, num_blocks, chain_length, cache_size, cache_block_size, prefetch_size)
                    
                    if (op_type == "write"):
                    	benchmark = WriteBenchmark(ht_clients, data_size, num_clients, num_ops)
                    if (op_type == "read"):
                        benchmark = ReadBenchmark(ht_clients, data_size, num_clients, num_ops)
                    else:
                        print >> data, "Incorrect operation type for file: ", op_type
                        return 0
                    
                    benchmark.run()
                    result = benchmark.wait()
                    client.remove(path)
                    
                    print >> data, "===== ", "Cache_Size= ", cache_size, "Prefetch Size= ", prefetch_size, " ======"
                    print >> data, "\t", num_ops, " requests completed in ", (float(num_ops) / result[0]), " s"
                    print >> data, "\t", num_clients, " parallel clients"
                    print >> data, "\t", data_size, " payload"
                    print >> data, "\tAverage write latency: ", result[1], "us"
                    print >> data, "\tAverage read latency: ", result[2], "us"
                    print >> data, "\tAverage total latency: ", result[1]+result[2], "us"
                    print >> data, "\tThroughput: ", result[0], " bytes per second"
                    print >> data, "\tHit_rate: ", round(result[3],4), "%"
                    print >> data, "\n"
                    loading += 1
                    print("Loading -- ", round(float(loading*100/110),1), "%")
            count *= 2

    data.close()
    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', "--mode", dest="mode", default="bp", 
                        help="""
                            Specify different variants. Use the following flags to specify: 'bp', 'bpz', 'cp', 'cpz', 'cb', cbz'.
                            Notice here 'b' = block size, 'c' = cache size, 'p' = prefetch size, 'z' = block visited by zipf distribution.
                            Default mode is bp. """)
    args = parser.parse_args()
    if args.mode == "bp":
        file_bp()
    elif args.mode == "bpz":
        file_bp_zipf()
    elif args.mode == "cp":
        file_cp()
    elif args.mode == "cpz":
        file_cp_zipf()
    elif args.mode == "cb":
        file_cb()
    elif args.mode == "cbz":
        file_cb_zipf()
    else:
        print("Wrong mode arguments!")
    