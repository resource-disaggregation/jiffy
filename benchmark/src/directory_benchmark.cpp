#include <vector>
#include <thread>
#include <boost/program_options.hpp>
#include <jiffy/directory/client/directory_client.h>
#include <jiffy/directory/client/lease_client.h>
#include <jiffy/client/jiffy_client.h>
#include <jiffy/utils/logger.h>
#include <jiffy/utils/signal_handling.h>
#include <jiffy/utils/time_utils.h>
#include <jiffy/utils/thread_utils.h>
#include <random>

using namespace ::jiffy::client;
using namespace ::jiffy::directory;
using namespace ::jiffy::storage;
using namespace ::jiffy::utils;

using namespace ::apache::thrift;

typedef std::shared_ptr<directory_client> directory_client_ptr;
typedef std::shared_ptr<lease_client> lease_client_ptr;
typedef std::vector<directory_client_ptr> d_client_list;
typedef std::vector<lease_client_ptr> l_client_list;
typedef std::vector<std::string> path_list;

class directory_benchmark {
 public:
  directory_benchmark(d_client_list &d_clients,
                 l_client_list &l_clients,
                 path_list &path,
                 size_t num_clients,
                 size_t num_ops)

      : path_(path),
        num_clients_(num_clients),
        num_ops_(num_ops / num_clients),
        d_clients_(d_clients),
        l_clients_(l_clients),
        workers_(num_clients),
        throughput_(num_clients),
        latency_(num_clients) {
  }

  virtual ~directory_benchmark() = default;

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
    for (size_t i = 0; i < num_clients_; ++i) {
      for(size_t j = 0; j < num_ops_; ++j) {
        try {
          d_clients_[i]->remove(std::string("/") + std::to_string(i) + std::string("_") +  std::to_string(j));
        } catch (const std::exception& ex) {
            continue;
        }
      }
    }
    return std::make_pair(throughput, latency / num_clients_);
  }

 protected:
  path_list &path_;
  size_t num_clients_;
  size_t num_ops_;
  d_client_list &d_clients_;
  l_client_list &l_clients_;
  std::vector<std::thread> workers_;
  std::vector<double> throughput_;
  std::vector<double> latency_;
};

class open_or_create_benchmark : public directory_benchmark {
 public:
  open_or_create_benchmark(d_client_list &d_clients,
                  l_client_list &l_clients,
                  path_list &path,
                  size_t num_clients,
                  size_t num_ops) : directory_benchmark(d_clients, l_clients, path, num_clients, num_ops) {
  }

  void run() override {
    // d_clients_[0]->open_or_create("/a", "hashtable", std::string("local/a"));
    for (size_t i = 0; i < num_clients_; ++i) {
      workers_[i] = std::thread([i, this]() {
        std::random_device rd; // obtain a random number from hardware
        std::mt19937 gen(rd()); // seed the generator
        std::uniform_int_distribution<uint64_t> distr(0, std::numeric_limits<uint64_t>::max() - 1); // define the range
        auto bench_begin = time_utils::now_us();
        uint64_t tot_time = 0, t0, t1 = bench_begin;
        size_t j;
        for (j = 0; j < num_ops_; ++j) {
          t0 = time_utils::now_us();
//          d_clients_[i]->open_or_create(std::string("/") + std::to_string(i) + std::string("/") +  std::to_string(j), "hashtable", std::string("local/") + path_[j] + std::string("-") + std::to_string(i));
          //d_clients_[i]->open(std::string("/") + std::to_string(i) + std::string("/") +  std::to_string(j));
        //   d_clients_[i]->open("/a");
            d_clients_[i]->create("/a/foo" + std::to_string(distr(gen)), "local://tmp");
          t1 = time_utils::now_us();
          tot_time += (t1 - t0);
        }
        latency_[i] = (double) tot_time / (double) j;
        throughput_[i] = j * 1E6 / (t1 - bench_begin);
      });
      thread_utils::set_core_affinity(workers_[i], i);
    }
  }
};

class add_block_benchmark : public directory_benchmark {
 public:
  add_block_benchmark(d_client_list &d_clients,
                 l_client_list &l_clients,
                 path_list &path,
                 size_t num_clients,
                 size_t num_ops) : directory_benchmark(d_clients, l_clients, path, num_clients, num_ops) {
  }

  void run() override {
    for (size_t i = 0; i < num_clients_; ++i) {
        for (size_t j = 0; j < num_ops_; ++j) {
          d_clients_[i]->open_or_create(std::string("/") + std::to_string(i) + std::string("_") +  std::to_string(j), "hashtable", std::string("local") + std::to_string(j) + std::string("-") + std::to_string(i));
        }
    }
    for (size_t i = 0; i < num_clients_; ++i) {
      workers_[i] = std::thread([i, this]() {
        auto bench_begin = time_utils::now_us();
        uint64_t tot_time = 0, t0, t1 = bench_begin;
        size_t j;
        for (j = 0; j < num_ops_; ++j) {
          t0 = time_utils::now_us();
          d_clients_[i]->add_block(std::string("/") + std::to_string(i) + std::string("_") +  std::to_string(j), std::to_string(j) + std::string("-") +  std::to_string(i), "regular");
          t1 = time_utils::now_us();
          tot_time += (t1 - t0);
        }
        latency_[i] = (double) tot_time / (double) j;
        throughput_[i] = (double) j * 1E6 / (double) (t1 - bench_begin);
      });
      thread_utils::set_core_affinity(workers_[i], i);
    }
  }

};

class renew_leases_benchmark : public directory_benchmark {
 public:
  renew_leases_benchmark(d_client_list &d_clients,
                 l_client_list &l_clients,
                 path_list &path,
                 size_t num_clients,
                 size_t num_ops) : directory_benchmark(d_clients, l_clients, path, num_clients, num_ops) {
  }

  void run() override {
    for (size_t i = 0; i < num_clients_; ++i) {
        for (size_t j = 0; j < num_ops_; ++j) {
          d_clients_[0]->open_or_create(std::string("/") + std::to_string(i) + std::string("_") +  std::to_string(j), "hashtable", std::string("local") + std::to_string(j) + std::string("-") + std::to_string(i));
        }
    }
    for (size_t i = 0; i < num_clients_; ++i) {
      workers_[i] = std::thread([i, this]() {
        auto bench_begin = time_utils::now_us();
        uint64_t tot_time = 0, t0, t1 = bench_begin;
        size_t j;
        for (j = 0; j < num_ops_; ++j) {
          std::vector<std::string> paths;
          paths.push_back(std::string("/") + std::to_string(i) + std::string("_") + std::to_string(j));
          t0 = time_utils::now_us();
          l_clients_[i]->renew_leases(paths);
          t1 = time_utils::now_us();
          tot_time += (t1 - t0);
        }
        latency_[i] = (double) tot_time / (double) j;
        throughput_[i] = (double) j * 1E6 / (double) (t1 - bench_begin);
      });
      thread_utils::set_core_affinity(workers_[i], i);
    }
  }

};

int main(int argc, const char **argv) {
  std::string address = "172.31.8.153";
  int service_port = 9090;
  int lease_port = 9091;
  int num_ops = 100000;
  int para = std::stoi(std::string(argv[1]));
  std::vector<std::string> op_type_set;
  op_type_set.push_back("open_or_create");
  //op_type_set.push_back("add_block");
  //op_type_set.push_back("renew_leases");
  //
  // THIS PATH IS NOT USED
  path_list paths;
  for(size_t i = 0;i < num_ops;i++) {
      paths.push_back(std::string("/a/") + std::to_string(i));
  }

  // Output all the configuration parameters:
  LOG(log_level::info) << "host: " << address;
  LOG(log_level::info) << "service-port: " << service_port;
  LOG(log_level::info) << "lease-port: " << lease_port;
  LOG(log_level::info) << "num-ops: " << num_ops;

  for (const auto &op_type:op_type_set) {
    for (int i = para; i <= para; i *= 2) {
      int num_clients = i;
      d_client_list d_clients(static_cast<size_t>(num_clients), nullptr);
      l_client_list l_clients(static_cast<size_t>(num_clients), nullptr);

      if(op_type == "open_or_create" || op_type == "add_block") {
        for (int j = 0; j < num_clients; ++j) {
          d_clients[j] = std::make_shared<directory_client>(address, service_port);
        }
      } else if(op_type == "renew_leases") {
          d_clients[0] = std::make_shared<directory_client>(address, service_port);
        for (int j = 0; j < num_clients; ++j) {
          l_clients[j] = std::make_shared<lease_client>(address, lease_port);
        }
      }

      std::shared_ptr<directory_benchmark> benchmark = nullptr;
      if (op_type == "open_or_create") {
        benchmark = std::make_shared<open_or_create_benchmark>(d_clients, l_clients, paths, num_clients, num_ops);
      } else if (op_type == "add_block") {
        benchmark = std::make_shared<add_block_benchmark>(d_clients, l_clients, paths, num_clients, num_ops);
      } else if (op_type == "renew_leases") {
        benchmark = std::make_shared<renew_leases_benchmark>(d_clients, l_clients, paths, num_clients, num_ops);
      } else {
        LOG(log_level::info) << "Incorrect operation type for file: " << op_type;
        return 0;
      }
      benchmark->run();
      auto result = benchmark->wait();
      LOG(log_level::info) << op_type << " result " << num_clients << " " << result.second << " " << result.first;
      LOG(log_level::info) << "===== " << op_type << " ======";
      LOG(log_level::info) << "\t" << num_ops << " requests completed in " << ((double) num_ops / result.first)
                           << " s";
      LOG(log_level::info) << "\t" << num_clients << " parallel clients";
      LOG(log_level::info) << "\tAverage latency: " << result.second << " us";
      LOG(log_level::info) << "\tThroughput: " << result.first << " requests per second";
    }
  }
  return 0;
}