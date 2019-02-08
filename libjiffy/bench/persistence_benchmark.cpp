#include <iostream>
#include <fstream>
#include <sstream>
#include "jiffy/utils/time_utils.h"
#include "jiffy/utils/signal_handling.h"
#include "jiffy/utils/logger.h"
#include "jiffy/utils/cmd_parse.h"
#include "jiffy/storage/hashtable/hash_table_partition.h"
#include "jiffy/storage/hashtable/hash_slot.h"
#include "benchmark_utils.h"

using namespace jiffy::storage;
using namespace jiffy::persistent;
using namespace jiffy::utils;
using namespace jiffy::benchmark;

std::string to_padded_string(size_t num, size_t padding) {
  std::ostringstream ss;
  ss << std::setw(static_cast<int>(padding)) << std::setfill('0') << num;
  std::string result = ss.str();
  if (result.length() > padding) {
    result.erase(0, result.length() - padding);
  }
  return result;
}

int main(int argc, char **argv) {
  signal_handling::install_error_handler(SIGABRT, SIGFPE, SIGSEGV, SIGILL, SIGTRAP);

  cmd_options opts;
  opts.add(cmd_option("num-keys", 'n', false).set_default("1024").set_description("Number of keys"));
  opts.add(cmd_option("path", 'p', false).set_default("local://tmp/test").set_description("Backing persistent path"));
  opts.add(cmd_option("format", 'f', false).set_default("csv").set_description("Serialization/Deserialization format"));
  opts.add(cmd_option("mode", 'm', false).set_default("write").set_description("Benchmark mode (write/read)"));

  cmd_parser parser(argc, argv, opts);
  if (parser.get_flag("help")) {
    std::cerr << parser.help_msg() << std::endl;
    return 0;
  }

  size_t num_keys;
  std::string path;
  std::string format;
  std::string mode;
  try {
    num_keys = static_cast<size_t>(parser.get_long("num-keys"));
    path = parser.get("path");
    format = parser.get("format");
    mode = parser.get("mode");
  } catch (cmd_parse_exception &ex) {
    std::cerr << "Could not parse command line args: " << ex.what() << std::endl;
    std::cerr << parser.help_msg() << std::endl;
    return -1;
  }
  std::shared_ptr<serde> fmt{};
  if (format == "csv") {
    LOG(log_level::info) << "Serialization/deserialization format: csv";
    fmt = std::make_shared<csv_serde>();
  } else if (format == "binary") {
    LOG(log_level::info) << "Serialization/deserialization format: binary";
    fmt = std::make_shared<binary_serde>();
  } else {
    LOG(log_level::error) << "Unknown Serialization/deserialization format " << format << "; terminating...";
    return -1;
  }

  block_memory_manager manager;
  hash_table_partition block(&manager);
  block.slot_range(0, hash_slot::MAX);

  if (mode == "write") {
    // Load phase
    LOG(log_level::info) << "Loading data...";
    for (size_t i = 0; i < num_keys; i++) {
      std::string key = to_padded_string(i, 22);
      std::string value = std::string(1000, 'x');
      block.put(key, value);
    }
    LOG(log_level::info) << "Loaded " << block.size() << " keys";

    LOG(log_level::info) << "Persisting to " << path;
    auto t0 = time_utils::now_ms();
    block.sync(path);
    auto t1 = time_utils::now_ms();
    LOG(log_level::info) << "Persisted to " << path << " in " << (t1 - t0) << " ms";
  } else if (mode == "read") {
    // Load phase
    LOG(log_level::info) << "Loading data from " << path << "...";
    auto t0 = time_utils::now_ms();
    block.load(path);
    auto t1 = time_utils::now_ms();
    LOG(log_level::info) << "Loaded " << block.size() << " keys from " << path << " in " << (t1 - t0) << " ms";
  } else {
    LOG(log_level::error) << "Unknown benchmark mode: " << mode;
  }

  return 0;
}
