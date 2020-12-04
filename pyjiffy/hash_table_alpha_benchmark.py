from jiffy import JiffyClient, b, Flags
from jiffy.benchmark.zipf_generator import gen_zipf
import time
import threading

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

class GetBenchmark(HashTableBenchmark):
	def __init__(self, clients, data_size, num_clients, num_ops, a):
		super(GetBenchmark, self).__init__(clients, data_size, num_clients, num_ops)
		self.zipf_sample = gen_zipf(a, num_ops, num_ops)

	def run(self):
		# print("+++++++++ Get Benchmark +++++++++")
		t0 = time.time()
		for i in range(self.num_clients):
			self.workers_[i] = threading.Thread(target = self.single_thread_action, args = (i,))
		t1 = time.time()
		# print("Time to Create Thread: ", t1-t0)
		t0 = time.time()
		for i in range(self.num_clients):
			self.workers_[i].start()
		t1 = time.time()
		# print("Time to Start Thread: ", t1-t0)
				
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
		for j in self.zipf_sample:
			t0 = time.time()
			cache_hit += self.clients_[thread_index].get(str(j) + "_" + str(thread_index))[1]
			t1 = time.time()
			get_time += (t1 - t0)
		# print("Operation Get: Runtime of Thread", thread_index, "= ", tot_time)
		self.cache_hit_[thread_index] = cache_hit 
		self.latency_get_[thread_index] = (10 ** 6) * float(get_time) / float(self.num_ops_)
		self.latency_put_[thread_index] = (10 ** 6) * float(put_time) / float(self.num_ops_)
		self.throughput_[thread_index] = self.num_ops_ / (t1 - get_begin)

def run_benchmark():
	address = "frog.zoo.cs.yale.edu"
	# address = "127.0.0.1"
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
	file_name = './benchmark/ht_alpha_with_cache.txt'
	data=open(file_name,'w+')
	# Output all the configuration parameters:
	print("host: ", address,file=data)
	print("service-port: ", service_port,file=data)
	print("lease-port: ", lease_port,file=data)
	print("num-blocks: ", num_blocks,file=data)
	print("chain-length: ", chain_length,file=data)
	print("num-ops: ", num_ops,file=data)
	print("data-size: ", data_size,file=data)
	print("path: ", path,file=data)
	print("backing-path: ", backing_path,file=data)

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
		print("===== ", "Zipf_ht_Benchmark, ","alpha= ", a, " ======", file=data)
		print("\t", num_ops, " requests completed in ", (float(num_ops) / result[0])
												, " s", file=data)
		print("\t", num_clients, " parallel clients", file=data)
		print("\t", data_size, " payload", file=data)
		print("\tAverage put latency: ", result[1], "us", file=data)
		print("\tAverage get latency: ", result[2], "us", file=data)
		print("\tAverage total latency: ", result[1]+result[2], "us", file=data)
		print("\tThroughput: ", result[0], " requests per second", file=data)
		print("\tHit_rate: ", round(result[3],4), "%", file=data)
		print("\n", file=data)
		loading += 1
		print("Loading -- ", round(float(loading*100/11),1), "%")
	
	return 0


if __name__ == "__main__":
	run_benchmark()
	