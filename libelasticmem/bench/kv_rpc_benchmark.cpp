#include <iostream>
#include <fstream>
#include <sstream>
#include "../src/storage/service/block_chain_client.h"
#include "../src/utils/time_utils.h"
#include "../src/utils/signal_handling.h"
#include "../src/utils/logger.h"
#include "../src/utils/cmd_parse.h"
#include "../src/storage/kv/kv_block.h"
#include "../src/directory/fs/directory_client.h"

using namespace elasticmem::storage;
using namespace elasticmem::utils;
using namespace ::apache::thrift;

static void load_workload(const std::string &workload_path,
                          std::size_t workload_offset,
                          std::size_t num_ops,
                          std::vector<std::pair<int32_t, std::vector<std::string>>> &workload) {
  workload.clear();
  std::ifstream in(workload_path);
  std::string line;
  std::cerr << "Loading workload from " << workload_path << std::endl;
  std::size_t line_num = 0;
  while (std::getline(in, line) && workload.size() < num_ops) {
    if (line_num++ < workload_offset)
      continue;
    std::istringstream iss(line);
    std::vector<std::string> tokens{std::istream_iterator<std::string>{iss},
                                    std::istream_iterator<std::string>{}};
    int32_t cmd_id = -1;
    if (tokens[0] == "get") {
      cmd_id = kv_op_id::get;
    } else if (tokens[0] == "put") {
      cmd_id = kv_op_id::put;
    } else if (tokens[0] == "remove") {
      cmd_id = kv_op_id::remove;
    } else if (tokens[0] == "update") {
      cmd_id = kv_op_id::update;
    } else if (tokens[0] == "wait") {
      continue;
    } else {
      throw std::logic_error("Unknown operation " + tokens[0]);
    }
    tokens.erase(tokens.begin());
    workload.emplace_back(cmd_id, tokens);
  }
  std::cerr << "Loaded " << workload.size() << " entries from " << workload_path << std::endl;
}

class throughput_benchmark {
 public:
  throughput_benchmark(const std::string &workload_path,
                       std::size_t workload_offset,
                       const std::vector<std::string> &chain,
                       std::size_t num_ops,
                       std::size_t max_async)
      : num_ops_(num_ops), max_async_(max_async), client_(chain) {
    load_workload(workload_path, workload_offset, num_ops, workload_);
  }

  void run() {
    worker_thread_ = std::thread([&]() {
      std::size_t i = 0;
      auto begin = time_utils::now_us();
      while (i < num_ops_) {
        client_.run_command(workload_[i].first, workload_[i].second);
        i++;
      }
      auto tot_time = time_utils::now_us() - begin;
      fprintf(stdout, "%lf\n", static_cast<double>(num_ops_) * 1e6 / static_cast<double>(tot_time));
    });
  }

  void wait() {
    if (worker_thread_.joinable()) {
      worker_thread_.join();
    }
  }

 private:
  std::thread worker_thread_;
  std::size_t num_ops_;
  std::size_t max_async_;
  std::vector<std::pair<int32_t, std::vector<std::string>>> workload_;
  block_chain_client client_;
};

class latency_benchmark {
 public:
  latency_benchmark(const std::string &workload_path,
                    std::size_t workload_offset,
                    const std::vector<std::string> &chain,
                    std::size_t num_ops)
      : num_ops_(num_ops), client_(chain) {
    load_workload(workload_path, workload_offset, num_ops, workload_);
  }

  void run() {
    std::size_t i = 0;
    time_utils::now_us();
    while (i < num_ops_) {
      auto t0 = time_utils::now_us();
      client_.run_command(workload_[i].first, workload_[i].second);
      auto t = time_utils::now_us() - t0;
      fprintf(stdout, "%zu %" PRId64 "\n", i, t);
      ++i;
    }
  }

 private:
  std::size_t num_ops_;
  std::vector<std::pair<int32_t, std::vector<std::string>>> workload_;
  block_chain_client client_;
};

int main(int argc, char **argv) {
  signal_handling::install_error_handler(SIGABRT, SIGFPE, SIGSEGV, SIGILL, SIGTRAP);

  GlobalOutput.setOutputFunction(log_utils::log_thrift_msg);

  cmd_options opts;
  opts.add(cmd_option("file-name", 'f', false).set_default("/benchmark").set_description("File to benchmark"));
  opts.add(cmd_option("dir-host", 'h', false).set_default("127.0.0.1").set_description("Directory service host"));
  opts.add(cmd_option("dir-port", 'p', false).set_default("/benchmark").set_description("Directory service port"));
  opts.add(cmd_option("benchmark-type", 'b', false).set_default("throughput").set_description("Benchmark type"));
  opts.add(cmd_option("num-threads", 't', false).set_default("1").set_description("# of benchmark threads to run"));
  opts.add(cmd_option("num-ops", 'n', false).set_default("100000").set_description("# of operations to run"));
  opts.add(cmd_option("max-async", 'm', false).set_default("1000").set_description(
      "Maximum number of unacknowledged requests in flight"));
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
  std::string benchmark_type;
  std::size_t num_threads;
  std::size_t num_ops;
  std::size_t max_async;
  std::string workload_path;
  std::size_t workload_offset;

  try {
    file = parser.get("file-name");
    host = parser.get("dir-host");
    port = parser.get_int("dir-port");
    benchmark_type = parser.get("benchmark-type");
    num_threads = static_cast<std::size_t>(parser.get_long("num-threads"));
    num_ops = static_cast<std::size_t>(parser.get_long("num-ops"));
    max_async = static_cast<std::size_t>(parser.get_long("max-async"));
    workload_path = parser.get("workload-path");
    workload_offset = static_cast<std::size_t>(parser.get_long("workload-offset"));
  } catch (cmd_parse_exception &ex) {
    std::cerr << "Could not parse command line args: " << ex.what() << std::endl;
    std::cerr << parser.help_msg() << std::endl;
    return -1;
  }

  elasticmem::directory::directory_client client(host, port);
  auto dstatus = client.open_or_create(file, "/tmp", 1, 1);

  if (benchmark_type == "throughput") {
    std::vector<throughput_benchmark *> benchmark;

    // Create
    for (std::size_t i = 0; i < num_threads; i++) {
      auto thread_ops = num_ops / num_threads;
      benchmark.push_back(new throughput_benchmark(workload_path,
                                                   workload_offset + i * thread_ops,
                                                   dstatus.data_blocks().front().block_names,
                                                   thread_ops,
                                                   max_async));
    }

    // Start
    for (std::size_t i = 0; i < num_threads; i++) {
      benchmark[i]->run();
    }

    // Wait
    for (std::size_t i = 0; i < num_threads; i++) {
      benchmark[i]->wait();
    }

    // Cleanup
    for (std::size_t i = 0; i < num_threads; i++) {
      delete benchmark[i];
    }
  } else if (benchmark_type == "latency") {
    latency_benchmark benchmark(workload_path, workload_offset, dstatus.data_blocks().front().block_names, num_ops);
    benchmark.run();
  }
}