#include <iostream>
#include <fstream>
#include <sstream>
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/time_utils.h"
#include "jiffy/utils/signal_handling.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/cmd_parse.h"
#include "jiffy/directory/client/directory_client.h"
#include "benchmark_utils.h"
#include "jiffy/storage/client/block_listener.h"
#include "jiffy/storage/client/hash_table_listener.h"
#include "jiffy/storage/client/hash_table_client.h"
#include "jiffy/client/jiffy_client.h"

using namespace jiffy::storage;
using namespace jiffy::utils;
using namespace jiffy::benchmark;
using namespace ::apache::thrift;

class workload_runner {
 public:
  workload_runner(const std::string &workload_path,
                  std::size_t workload_offset,
                  std::shared_ptr<hash_table_client> hash_table,
                  std::size_t num_ops) : num_ops_(num_ops), hash_table_(hash_table) {
    benchmark_utils::load_workload(workload_path, workload_offset, num_ops, workload_);
    timestamps_.resize(workload_.size());
  }

  void run() {
    std::size_t i = 0;
    while (i < num_ops_) {
      timestamps_[i] = time_utils::now_us();
      hash_table_->put(workload_[i].second[0], workload_[i].second[1]);
      ++i;
    }
  }

  const std::vector<std::uint64_t> &timestamps() const {
    return timestamps_;
  }

 private:
  std::vector<std::uint64_t> timestamps_{};
  std::size_t num_ops_;
  std::vector<std::pair<int32_t, std::vector<std::string>>> workload_{};
  std::shared_ptr<hash_table_client> hash_table_;
};

class notification_listener {
 public:
  notification_listener(jiffy::client::jiffy_client &client, const std::string &file, std::size_t num_ops)
      : listener_(client.listen(file)),
        num_ops_(num_ops) {
    listener_->subscribe({"put"});
    timestamps_.resize(num_ops_);
  }

  void run() {
    worker_ = std::thread([&] {
      std::size_t i = 0;
      while (i < num_ops_) {
        listener_->get_notification();
        timestamps_[i++] = time_utils::now_us();
      }
    });
  }

  void wait() {
    worker_.join();
  }

  const std::vector<std::uint64_t> &timestamps() const {
    return timestamps_;
  }

 private:
  std::thread worker_;
  std::shared_ptr<hash_table_listener> listener_;
  std::size_t num_ops_;
  std::vector<std::uint64_t> timestamps_{};
};

int main(int argc, char **argv) {
  signal_handling::install_error_handler(SIGABRT, SIGFPE, SIGSEGV, SIGILL, SIGTRAP);

  GlobalOutput.setOutputFunction(log_utils::log_thrift_msg);

  cmd_options opts;
  opts.add(cmd_option("file-name", 'f', false).set_default("/benchmark").set_description("File to benchmark"));
  opts.add(cmd_option("dir-host", 'H', false).set_default("127.0.0.1").set_description("Directory service host"));
  opts.add(cmd_option("dir-port", 'P', false).set_default("9090").set_description("Directory service port"));
  opts.add(cmd_option("lease-port", 'L', false).set_default("9091").set_description("Lease service port"));
  opts.add(cmd_option("chain-length", 'c', false).set_default("1").set_description("Chain length"));
  opts.add(cmd_option("num-threads", 't', false).set_default("1").set_description("# of benchmark threads to run"));
  opts.add(cmd_option("num-ops", 'n', false).set_default("100000").set_description("# of operations to run"));
  opts.add(cmd_option("workload-path", 'w', false).set_default("data").set_description(
      "Path to read the workload from"));
  opts.add(cmd_option("workload-offset", 'o', false).set_default("0").set_description(
      "Offset (line-number) into the workload to start from"));

  cmd_parser parser(argc, argv, opts);
  if (parser.get_flag("help")) {
    std::cerr << parser.help_msg() << std::endl;
    return 0;
  }

  std::string file;
  std::string host;
  int port;
  int lease_port;
  std::size_t num_threads;
  std::size_t num_ops;
  std::string workload_path;
  std::size_t workload_offset;
  std::size_t chain_length;

  try {
    file = parser.get("file-name");
    host = parser.get("dir-host");
    port = parser.get_int("dir-port");
    lease_port = parser.get_int("lease-port");
    chain_length = static_cast<std::size_t>(parser.get_long("chain-length"));
    num_threads = static_cast<std::size_t>(parser.get_long("num-threads"));
    num_ops = static_cast<std::size_t>(parser.get_long("num-ops"));
    workload_path = parser.get("workload-path");
    workload_offset = static_cast<std::size_t>(parser.get_long("workload-offset"));
  } catch (cmd_parse_exception &ex) {
    std::cerr << "Could not parse command line args: " << ex.what() << std::endl;
    std::cerr << parser.help_msg() << std::endl;
    return -1;
  }

  jiffy::client::jiffy_client client(host, port, lease_port);
  auto hash_table = client.open_or_create_hash_table(file, "/tmp", 1, chain_length);

  // Create workload runner
  std::cerr << "Creating workload runner" << std::endl;
  workload_runner w_runner(workload_path, workload_offset, hash_table, num_ops);

  // Create all listeners and start them
  std::vector<notification_listener *> listeners(num_threads, nullptr);
  for (std::size_t i = 0; i < num_threads; ++i) {
    listeners[i] = new notification_listener(client, file, num_ops);
    listeners[i]->run();
  }

  // Start workload runner
  std::cerr << "Starting workload runner" << std::endl;
  w_runner.run();

  // Do all the measurements
  std::cerr << "Finished" << std::endl;
  for (std::size_t i = 0; i < num_threads; ++i) {
    auto l = listeners[i];
    l->wait();
    auto out_file = "listen_latency_" + std::to_string(i) + "_of_" + std::to_string(num_threads);
    benchmark_utils::vector_diff(l->timestamps(), w_runner.timestamps(), out_file);
    delete l;
  }
}
