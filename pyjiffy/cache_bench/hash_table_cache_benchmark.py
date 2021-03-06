from jiffy import JiffyClient, b, Flags
from jiffy.benchmark.zipf_generator import gen_zipf
import time
import threading
import argparse

class HashTableBenchmark:
    	def __init__(self, clients, data_size, num_clients, num_ops):
		self.data_ = "x" * data_size
		self.num_clients = num_clients
		self.num_ops_ = int(num_ops / num_clients)
		self.clients_ = clients
		self.workers_ = [None] * self.num_clients
		self.cache_hit_ = [None] * self.num_clients
		self.throughput_ = [None] * self.num_clients
		self.latency_put_ = [None] * self.num_clients
		self.latency_get_ = [None] * self.num_clients

	def wait(self):
		throughput = 0.0
		latency_put = 0.0
		latency_get = 0.0
		hit = 0
		for i in range(self.num_clients):
			self.workers_[i].join()
			throughput += self.throughput_[i]
			latency_put += self.latency_put_[i]
			latency_get += self.latency_get_[i]
			hit += self.cache_hit_[i]
		return [throughput, latency_put / self.num_clients, latency_get / self.num_clients, float(hit * 100 / self.num_ops_)]

class PutBenchmark(HashTableBenchmark):
	def __init__(self, clients, data_size, num_clients, num_ops):
		super(PutBenchmark, self).__init__(clients, data_size, num_clients, num_ops)

	def run(self):
		t0 = time.time()
		for i in range(self.num_clients):
			self.workers_[i] = threading.Thread(target = self.single_thread_action, args = (i,))
		t1 = time.time()
		t0 = time.time()
		for i in range(self.num_clients):
			self.workers_[i].start()
		t1 = time.time()
				
	def single_thread_action(self, thread_index):
		bench_begin = time.time()
		tot_time = 0.0
		t0, t1 = bench_begin, bench_begin
		for j in range(self.num_ops_):
			t0 = time.time()
			self.clients_[thread_index].put(str(j) + "_" + str(thread_index), self.data_)
			t1 = time.time()
			tot_time += (t1 - t0)
		j += 1
		self.latency_[thread_index] = (10 ** 6) * float(tot_time) / float(j)
		self.throughput_[thread_index] = j / (t1 - bench_begin)


class GetBenchmark(HashTableBenchmark):
	def __init__(self, clients, data_size, num_clients, num_ops):
		super(GetBenchmark, self).__init__(clients, data_size, num_clients, num_ops)

	def run(self):
		t0 = time.time()
		for i in range(self.num_clients):
			self.workers_[i] = threading.Thread(target = self.single_thread_action, args = (i,))
		t1 = time.time()
		t0 = time.time()
		for i in range(self.num_clients):
			self.workers_[i].start()
		t1 = time.time()
				
	def single_thread_action(self, thread_index):
		cache_hit = 0
		put_begin = time.time()
		put_time = 0.0
		t0, t1 = put_begin, put_begin
		for j in range(self.num_ops_):
			t0 = time.time()
			self.clients_[thread_index].put(str(j) + "_" + str(thread_index), self.data_)
			t1 = time.time()
			put_time += (t1 - t0)
		get_begin = time.time()
		get_time = 0.0
		t0, t1 = get_begin, get_begin
		for j in range(self.num_ops_):
			t0 = time.time()
			cache_hit += self.clients_[thread_index].get(str(j) + "_" + str(thread_index))[1]
			t1 = time.time()
			get_time += (t1 - t0)
		self.cache_hit_[thread_index] = cache_hit 
		self.latency_get_[thread_index] = (10 ** 6) * float(get_time) / float(j)
		self.latency_put_[thread_index] = (10 ** 6) * float(put_time) / float(j)
		self.throughput_[thread_index] = self.num_ops_ / (t1 - get_begin)

class RemoveBenchmark(HashTableBenchmark):
	def __init__(self, clients, data_size, num_clients, num_ops):
		super(RemoveBenchmark, self).__init__(clients, data_size, num_clients, num_ops)

	def run(self):
		t0 = time.time()
		for i in range(self.num_clients):
			self.workers_[i] = threading.Thread(target = self.single_thread_action, args = (i,))
		t1 = time.time()
		t0 = time.time()
		for i in range(self.num_clients):
			self.workers_[i].start()
		t1 = time.time()
				
	def single_thread_action(self, thread_index):
		for j in range(self.num_ops_):
			self.clients_[thread_index].put(str(j) + "_" + str(thread_index), self.data_)
		bench_begin = time.time()
		tot_time = 0.0
		t0, t1 = bench_begin, bench_begin
		for j in range(self.num_ops_):
			t0 = time.time()
			self.clients_[thread_index].remove(str(j) + "_" + str(thread_index))
			t1 = time.time()
			tot_time += (t1 - t0)
		j += 1
		self.latency_[thread_index] = (10 ** 6) * float(tot_time) / float(j)
		self.throughput_[thread_index] = j / (t1 - bench_begin)

def ht_alpha():
    address = "frog.zoo.cs.yale.edu"
    service_port = 9090
    lease_port = 9091
    num_blocks = 1
    chain_length = 1
    num_ops = 100000
    data_size = 64
    op_type_set = []
    op_type_set.append("get")
    path = "/tmp"
    backing_path = "local://tmp"
    file_name = './ht_alpha_with_cache.txt'
    data = open(file_name,'w+')
    # Output all the configuration parameters:
    print >> data, "host: ", address
    print >> data, "service-port: ", service_port
    print >> data, "lease-port: ", lease_port
    print >> data, "num-blocks: ", num_blocks
    print >> data, "chain-length: ", chain_length
    print >> data, "num-ops: ", num_ops
    print >> data, "data-size: ", data_size
    print >> data, "path: ", path
    print >> data, "backing-path: ", backing_path

    num_clients = 1
    loading = 0
    client = JiffyClient(address, service_port, lease_port)
    ht_clients = [None] * num_clients
    cache_size = num_ops//2*64
    for a in range(0,11):
        alpha = float(a/10)
        ht_clients[0] = client.open_or_create_hash_table(path, backing_path, num_blocks, chain_length, cache_size)
        benchmark = GetBenchmark(ht_clients, data_size, num_clients, num_ops, alpha)
        benchmark.run()
        result = benchmark.wait()
        client.remove(path)
        print >> data, "===== ", "Zipf_ht_Benchmark, ","alpha= ", a, " ======"
        print >> data, "\t", num_ops, " requests completed in ", (float(num_ops) / result[0]), " s"
        print >> data, "\t", num_clients, " parallel clients"
        print >> data, "\t", data_size, " payload"
        print >> data, "\tAverage put latency: ", result[1], "us"
        print >> data, "\tAverage get latency: ", result[2], "us"
        print >> data, "\tAverage total latency: ", result[1]+result[2], "us"
        print >> data, "\tThroughput: ", result[0], " requests per second"
        print >> data, "\tHit_rate: ", round(result[3],4), "%"
        print >> data, "\n"
        loading += 1
        print("Loading -- ", round(float(loading*100/11),1), "%")

    return 0

def ht_seq():
    address = "frog.zoo.cs.yale.edu"
    service_port = 9090
    lease_port = 9091
    num_blocks = 1
    chain_length = 1
    num_ops = 100000
    data_size = 64
    op_type_set = []
    op_type_set.append("put")
    op_type_set.append("get")
    op_type_set.append("remove")
    path = "/tmp"
    backing_path = "local://tmp"
    file_name = './ht_with_cache.txt'
    data = open(file_name,'w+')

    # Output all the configuration parameters:
    print >> data, "host: ", address
    print >> data, "service-port: ", service_port
    print >> data, "lease-port: ", lease_port
    print >> data, "num-blocks: ", num_blocks
    print >> data, "chain-length: ", chain_length
    print >> data, "num-ops: ", num_ops
    print >> data, "data-size: ", data_size
    print >> data, "path: ", path
    print >> data, "backing-path: ", backing_path

    loading = 0
    for op_type in op_type_set:
        count = 1
        while count <= 1:
            num_clients = count
            client = JiffyClient(address, service_port, lease_port)
            ht_clients = [None] * num_clients
            for cache_size in range(num_ops//20*64, num_ops*64+1, num_ops//20*64): 
                for i in range(num_clients): 
                    ht_clients[i] = client.open_or_create_hash_table(path, backing_path, num_blocks, chain_length, cache_size)
                
                if (op_type == "put"):
                	benchmark = PutBenchmark(ht_clients, data_size, num_clients, num_ops)
                if (op_type == "get"):
                    benchmark = GetBenchmark(ht_clients, data_size, num_clients, num_ops)
                elif (op_type == "remove"):
                	benchmark = RemoveBenchmark(ht_clients, data_size, num_clients, num_ops)
                else:
                    print("Incorrect operation type for hash table: ", op_type)
                    return 0
                
                benchmark.run()
                result = benchmark.wait()
                client.remove(path)
                print >> data, "===== ", "ht_Benchmark, ","Cache_Size= ", cache_size, " ======"
                print >> data, "\t", num_ops, " requests completed in ", (float(num_ops) / result[0]), " s"
                print >> data, "\t", num_clients, " parallel clients"
                print >> data, "\t", data_size, " payload"
                print >> data, "\tAverage put latency: ", result[1], "us"
                print >> data, "\tAverage get latency: ", result[2], "us"
                print >> data, "\tAverage total latency: ", result[1]+result[2], "us"
                print >> data, "\tThroughput: ", result[0], " requests per second"
                print >> data, "\tHit_rate: ", round(result[3],4), "%"
                print >> data, "\n"
                loading += 1
                print("Loading -- ", round(float(loading*100/20),1), "%")

            count *= 2

    return 0

def ht_zipf():
    address = "frog.zoo.cs.yale.edu"
    service_port = 9090
    lease_port = 9091
    num_blocks = 1
    chain_length = 1
    num_ops = 100000
    data_size = 64
    op_type_set = []

    op_type_set.append("get")
    path = "/tmp"
    backing_path = "local://tmp"
    file_name = './ht_zipf_with_cache.txt'
    data = open(file_name,'w+')

    # Output all the configuration parameters:
    print >> data, "host: ", address
    print >> data, "service-port: ", service_port
    print >> data, "lease-port: ", lease_port
    print >> data, "num-blocks: ", num_blocks
    print >> data, "chain-length: ", chain_length
    print >> data, "num-ops: ", num_ops
    print >> data, "data-size: ", data_size
    print >> data, "path: ", path
    print >> data, "backing-path: ", backing_path

    num_clients = 1
    loading = 0
    client = JiffyClient(address, service_port, lease_port)
    ht_clients = [None] * num_clients
    for cache_size in range(num_ops//20*64, num_ops*64+1, num_ops//20*64): 
        ht_clients[0] = client.open_or_create_hash_table(path, backing_path, num_blocks, chain_length, cache_size)
        benchmark = GetBenchmark(ht_clients, data_size, num_clients, num_ops)
        benchmark.run()
        result = benchmark.wait()
        client.remove(path)
        print >> data, "===== ", "Zipf_ht_Benchmark, ","Cache_Size= ", cache_size, " ======"
        print >> data, "\t", num_ops, " requests completed in ", (float(num_ops) / result[0]), " s"
        print >> data, "\t", num_clients, " parallel clients"
        print >> data, "\t", data_size, " payload"
        print >> data, "\tAverage put latency: ", result[1], "us"
        print >> data, "\tAverage get latency: ", result[2], "us"
        print >> data, "\tAverage total latency: ", result[1]+result[2], "us"
        print >> data, "\tThroughput: ", result[0], " requests per second"
        print >> data, "\tHit_rate: ", round(result[3],4), "%"
        print >> data, "\n"
        loading += 1
        print("Loading -- ", round(float(loading*100/20),1), "%")


    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', "--mode", dest="mode", default="s", 
                        help="""
                            Specify different variants. Use the following flags to specify: 'a', 'z', 's'.
                            Notice here 'a' = Test on alpha value of Zipf distribution, 's' = ht elements visited sequentially, 'z' = ht elements visited by zipf distribution.
                            Default mode is s. """)
    args = parser.parse_args()
    if args.mode == "a":
        ht_alpha()
    elif args.mode == "z":
        ht_zipf()
    elif args.mode == "s":
        ht_seq()
    else:
        print("Wrong mode arguments!")