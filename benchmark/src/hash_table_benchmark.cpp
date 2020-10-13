#include <vector>
#include <thread>
#include <boost/program_options.hpp>
#include <jiffy/client/jiffy_client.h>
#include <jiffy/utils/logger.h>
#include <jiffy/utils/signal_handling.h>
#include <jiffy/utils/time_utils.h>

using namespace ::jiffy::client;
using namespace ::jiffy::directory;
using namespace ::jiffy::storage;
using namespace ::jiffy::utils;

using namespace ::apache::thrift;

typedef std::shared_ptr<hash_table_client> writer_ptr;
typedef std::vector<writer_ptr> writer_list;

class hash_table_benchmark {
 public:
  hash_table_benchmark(writer_list &clients,
                       size_t data_size,
                       size_t num_clients,
                       size_t num_ops)
      : data_(data_size, 'x'),
        num_clients_(num_clients),
        num_ops_(num_ops / num_clients),
        clients_(clients),
        workers_(num_clients),
        throughput_(num_clients),
        latency_(num_clients) {
  }

  virtual ~hash_table_benchmark() = default;

  virtual void run() = 0;

  std::pair<double, double> wait() {
    double throughput = 0;
    double latency = 0;
    for (size_t i = 0; i < num_clients_; ++i) {
      if (workers_[i].joinable())
        workers_[i].join();
      throughput += throughput_[i];
      latency += latency_[i];
    }
    return std::make_pair(throughput, latency / num_clients_);
  }

 protected:
  std::string data_;
  size_t num_clients_;
  size_t num_ops_;
  std::vector<std::shared_ptr<hash_table_client>> &clients_;
  std::vector<std::thread> workers_;
  std::vector<double> throughput_;
  std::vector<double> latency_;
};

class put_benchmark : public hash_table_benchmark {
 public:
  put_benchmark(writer_list &clients,
                size_t data_size,
                size_t num_clients,
                size_t num_ops) : hash_table_benchmark(clients, data_size, num_clients, num_ops) {
  }

  void run() override {
    for (size_t i = 0; i < num_clients_; ++i) {
      workers_[i] = std::thread([i, this]() {
        auto bench_begin = time_utils::now_us();
        uint64_t tot_time = 0, t0, t1 = bench_begin;
        size_t j;
        for (j = 0; j < num_ops_; ++j) {
          t0 = time_utils::now_us();
          clients_[i]->put(std::to_string(j) + "_" + std::to_string(i), data_);
          t1 = time_utils::now_us();
          tot_time += (t1 - t0);
        }
        LOG(log_level::info) << "total_time: " << tot_time;
        LOG(log_level::info) << "j= " << j;
        latency_[i] = (double) tot_time / (double) j;
        throughput_[i] = j * 1E6 / (t1 - bench_begin);
      });
    }
  }
};

class get_benchmark : public hash_table_benchmark {
 public:
  get_benchmark(writer_list &clients,
                size_t data_size,
                size_t num_clients,
                size_t num_ops) : hash_table_benchmark(clients, data_size, num_clients, num_ops) {
  }

  void run() override {
    for (size_t i = 0; i < num_clients_; ++i) {
      workers_[i] = std::thread([i, this]() {
        for (size_t j = 0; j < num_ops_; ++j) {
          clients_[i]->put(std::to_string(j) + "_" + std::to_string(i), data_);
        }
        auto bench_begin = time_utils::now_us();
        uint64_t tot_time = 0, t0, t1 = bench_begin;
        size_t j;
        for (j = 0; j < num_ops_; ++j) {
          t0 = time_utils::now_us();
          clients_[i]->get(std::to_string(j) + "_" + std::to_string(i));
          t1 = time_utils::now_us();
          tot_time += (t1 - t0);
        }
        latency_[i] = (double) tot_time / (double) j;
        throughput_[i] = (double) j * 1E6 / (double) (t1 - bench_begin);
      });
    }
  }
};

class remove_benchmark : public hash_table_benchmark {
 public:
  remove_benchmark(writer_list &clients,
                   size_t data_size,
                   size_t num_clients,
                   size_t num_ops) : hash_table_benchmark(clients, data_size, num_clients, num_ops) {
  }

  void run() override {
    for (size_t i = 0; i < num_clients_; ++i) {
      workers_[i] = std::thread([i, this]() {
        for (size_t j = 0; j < num_ops_; ++j) {
          clients_[i]->put(std::to_string(j) + "_" + std::to_string(i), data_);
        }
        auto bench_begin = time_utils::now_us();
        uint64_t tot_time = 0, t0, t1 = bench_begin;
        size_t j;
        for (j = 0; j < num_ops_; ++j) {
          t0 = time_utils::now_us();
          clients_[i]->remove(std::to_string(j) + "_" + std::to_string(i));
          t1 = time_utils::now_us();
          tot_time += (t1 - t0);
        }
        latency_[i] = (double) tot_time / (double) j;
        throughput_[i] = (double) j * 1E6 / (double) (t1 - bench_begin);
      });
    }
  }
};

int main() {
  std::string address = "127.0.0.1";
  int service_port = 9090;
  int lease_port = 9091;
  int num_blocks = 1;
  int chain_length = 1;
  int num_ops = 100000;
  int data_size = 64;
  std::vector<std::string> op_type_set;
  op_type_set.push_back("remove");
  op_type_set.push_back("put");
  op_type_set.push_back("get");
  
  std::string path = "/tmp";
  std::string backing_path = "local://tmp";
  // Output all the configuration parameters:
  LOG(log_level::info) << "host: " << address;
  LOG(log_level::info) << "service-port: " << service_port;
  LOG(log_level::info) << "lease-port: " << lease_port;
  LOG(log_level::info) << "num-blocks: " << num_blocks;
  LOG(log_level::info) << "chain-length: " << chain_length;
  LOG(log_level::info) << "num-ops: " << num_ops;
  LOG(log_level::info) << "data-size: " << data_size;
  LOG(log_level::info) << "path: " << path;
  LOG(log_level::info) << "backing-path: " << backing_path;
  for (const auto &op_type:op_type_set) {

    for (int i = 1; i <= 64; i *= 2) {
      int num_clients = i;

      jiffy_client client(address, service_port, lease_port);

      std::vector<std::shared_ptr<hash_table_client>> ht_clients(static_cast<size_t>(num_clients), nullptr);
      for (int i = 0; i < num_clients; ++i) {
        ht_clients[i] = client.open_or_create_hash_table(path, backing_path, num_blocks, chain_length);
      }

      std::shared_ptr<hash_table_benchmark> benchmark = nullptr;
      if (op_type == "put") {
        benchmark = std::make_shared<put_benchmark>(ht_clients, data_size, num_clients, num_ops);
      } else if (op_type == "get") {
        benchmark = std::make_shared<get_benchmark>(ht_clients, data_size, num_clients, num_ops);
      } else if (op_type == "remove") {
        benchmark = std::make_shared<remove_benchmark>(ht_clients, data_size, num_clients, num_ops);
      } else {
        LOG(log_level::info) << "Incorrect operation type for hash table: " << op_type;
        return 0;
      }
      benchmark->run();
      auto result = benchmark->wait();
      client.remove(path);
      LOG(log_level::info) << op_type << " " << num_clients << " " << result.second << " " << result.first;
      LOG(log_level::info) << "===== " << op_type << " ======";
      LOG(log_level::info) << "\t" << num_ops << " requests completed in " << ((double) num_ops / result.first)
                           << " us";
      LOG(log_level::info) << "\t" << num_clients << " parallel clients";
      LOG(log_level::info) << "\t" << data_size << " payload";
      LOG(log_level::info) << "\tAverage latency: " << result.second;
      LOG(log_level::info) << "\tThroughput: " << result.first << " requests per microsecond";
    }
  }
  return 0;
}