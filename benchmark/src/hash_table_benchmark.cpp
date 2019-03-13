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

typedef std::shared_ptr<hash_table_client> client_ptr;
typedef std::vector<client_ptr> client_list;

class hash_table_benchmark {
 public:
  hash_table_benchmark(client_list &clients,
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
  put_benchmark(client_list &clients,
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
          clients_[i]->put(std::to_string(j), data_);
          t1 = time_utils::now_us();
          tot_time += (t1 - t0);
        }
        latency_[i] = (double) tot_time / (double) j;
        throughput_[i] = j * 1E6 / (t1 - bench_begin);
      });
    }
  }
};

class get_benchmark : public hash_table_benchmark {
 public:
  get_benchmark(client_list &clients,
                size_t data_size,
                size_t num_clients,
                size_t num_ops) : hash_table_benchmark(clients, data_size, num_clients, num_ops) {
  }

  void run() override {
    for (size_t i = 0; i < num_clients_; ++i) {
      workers_[i] = std::thread([i, this]() {
        for (size_t j = 0; j < num_ops_; ++j) {
          clients_[i]->put(std::to_string(j), data_);
        }
        auto bench_begin = time_utils::now_us();
        uint64_t tot_time = 0, t0, t1 = bench_begin;
        size_t j;
        for (j = 0; j < num_ops_; ++j) {
          t0 = time_utils::now_us();
          clients_[i]->get(std::to_string(j));
          t1 = time_utils::now_us();
          tot_time += (t1 - t0);
        }
        latency_[i] = (double) tot_time / (double) j;
        throughput_[i] = (double) j / (double) (t1 - bench_begin);
      });
    }
  }
};

int main(int argc, char **argv) {
  signal_handling::install_error_handler(SIGABRT, SIGFPE, SIGSEGV, SIGILL, SIGTRAP);

  GlobalOutput.setOutputFunction(log_utils::log_thrift_msg);

  // Parse configuration parameters
  // First set defaults
  std::string address = "127.0.0.1";
  int service_port = 9090;
  int lease_port = 9091;
  int num_clients = 1;
  int num_blocks = 1;
  int chain_length = 1;
  int num_ops = 100000;
  int data_size = 8;
  std::string op_type = "put";
  std::string path = "/tmp";
  std::string backing_path = "local://tmp";
  try {
    namespace po = boost::program_options;
    std::string config_file = "";
    po::options_description generic("options");
    generic.add_options()
        ("version,v", "Print version string")
        ("help,h", "Print help message")
        ("host", po::value<std::string>(&address)->default_value("127.0.0.1"))
        ("service-port", po::value<int>(&service_port)->default_value(9090))
        ("lease-port", po::value<int>(&lease_port)->default_value(9091))
        ("num-clients", po::value<int>(&num_clients)->default_value(1))
        ("num-blocks", po::value<int>(&num_blocks)->default_value(1))
        ("chain-length", po::value<int>(&chain_length)->default_value(1))
        ("num-ops", po::value<int>(&num_ops)->default_value(100000))
        ("data-size", po::value<int>(&data_size)->default_value(8))
        ("test", po::value<std::string>(&op_type)->default_value("put"))
        ("path", po::value<std::string>(&path)->default_value("/tmp"))
        ("backing-path", po::value<std::string>(&backing_path)->default_value("local://tmp"));

    po::options_description cmdline_options;
    cmdline_options.add(generic);

    po::options_description visible;
    visible.add(generic);

    po::variables_map vm;

    // Commandline args have highest priority
    store(po::command_line_parser(argc, argv).options(cmdline_options).run(), vm);
    notify(vm);

    if (vm.count("help")) {
      std::cout << "Directory service daemon" << std::endl;
      std::cout << visible << std::endl;
      return 0;
    }

    if (vm.count("version")) {
      std::cout << "Jiffy Hash Table Benchmark, Version 0.1.0" << std::endl; // TODO: Configure version string
      return 0;
    }

    // Output all the configuration parameters:
    LOG(log_level::info) << "host: " << address;
    LOG(log_level::info) << "service-port: " << service_port;
    LOG(log_level::info) << "lease-port: " << lease_port;
    LOG(log_level::info) << "num-clients: " << num_clients;
    LOG(log_level::info) << "num-blocks: " << num_blocks;
    LOG(log_level::info) << "chain-length: " << chain_length;
    LOG(log_level::info) << "num-ops: " << num_ops;
    LOG(log_level::info) << "data-size: " << data_size;
    LOG(log_level::info) << "test: " << op_type;
    LOG(log_level::info) << "path: " << path;
    LOG(log_level::info) << "backing-path: " << backing_path;

  } catch (std::exception &e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }

  jiffy_client client(address, service_port, lease_port);

  std::vector<std::shared_ptr<hash_table_client>> ht_clients(static_cast<size_t>(num_clients), nullptr);
  for (int i = 0; i < num_clients; ++i) {
    ht_clients[i] = client.open_or_create_hash_table(path, backing_path, num_blocks, chain_length);
  }

  std::shared_ptr<hash_table_benchmark> benchmark = nullptr;
  if (op_type == "put") {
    benchmark = std::make_shared<put_benchmark>(ht_clients, data_size, num_clients, num_ops);
  } else {
    benchmark = std::make_shared<get_benchmark>(ht_clients, data_size, num_clients, num_ops);
  }
  benchmark->run();
  auto result = benchmark->wait();
  client.remove(path);

  LOG(log_level::info) << "===== " << op_type << " ======";
  LOG(log_level::info) << "\t" << num_ops << " requests completed in " << ((double) num_ops / result.first) << " us";
  LOG(log_level::info) << "\t" << num_clients << " parallel clients";
  LOG(log_level::info) << "\t" << data_size << " payload";
  LOG(log_level::info) << "\tAverage latency: " << result.second;
  LOG(log_level::info) << "\tThroughput: " << result.first << " requests per microsecond";

  return 0;
}