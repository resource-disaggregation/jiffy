#include <vector>
#include <thread>
#include <boost/program_options.hpp>
#include <jiffy/client/jiffy_client.h>
#include <jiffy/utils/logger.h>
#include <jiffy/utils/signal_handling.h>
#include <jiffy/utils/time_utils.h>
#include "./zipf_generator.h"
#include <sstream>
#include <algorithm>
#include <cmath>
#include <ctime>
#include <cstdlib>
#include <fstream>
#include <atomic>
#include <chrono>

using namespace ::jiffy::client;
using namespace ::jiffy::directory;
using namespace ::jiffy::storage;
using namespace ::jiffy::utils;
using namespace ::apache::thrift;

std::vector<std::string> keygenerator(std::size_t num_keys, double theta = 0, int num_buckets = 512) {
  int bucket_size = 65536 / num_buckets;
  hash_slot hashslot;
  zipfgenerator zipf(theta, num_buckets);
  std::vector<uint64_t> zipfresult;
  std::map<uint64_t, uint64_t> bucket_dist;
  for (std::size_t i = 0; i < num_keys; i++) {
    bucket_dist[zipf.next()]++;
  }

  std::uint64_t count = 1;
  std::vector<std::string> keys;
  while (keys.size() != num_keys) {
    auto key = std::to_string(count);
    auto bucket = std::uint64_t(hashslot.get(key) / bucket_size);
    auto it = bucket_dist.find(bucket);
    if (it != bucket_dist.end()) {
      it->second = it->second - 1;
      keys.push_back(key);
      if (it->second == 0) {
        bucket_dist.erase(it);
      }
    }
    count++;
  }
  return keys;
}

int main() {
  size_t num_ops = 419430;
  std::vector<std::string> keys = keygenerator(num_ops);
  std::string address = "127.0.0.1";
  int service_port = 9090;
  int lease_port = 9091;
  int num_blocks = 1;
  int chain_length = 1;
  std::string op_type = "hash_table_auto_scaling";
  std::string path = "/tmp";
  std::string backing_path = "local://tmp";

  // Output all the configuration parameters:
  LOG(log_level::info) << "host: " << address;
  LOG(log_level::info) << "service-port: " << service_port;
  LOG(log_level::info) << "lease-port: " << lease_port;
  LOG(log_level::info) << "num-blocks: " << num_blocks;
  LOG(log_level::info) << "chain-length: " << chain_length;
  LOG(log_level::info) << "num-ops: " << num_ops;
  LOG(log_level::info) << "test: " << op_type;
  LOG(log_level::info) << "path: " << path;
  LOG(log_level::info) << "backing-path: " << backing_path;

  jiffy_client client(address, service_port, lease_port);
  std::shared_ptr<hash_table_client>
      ht_client = client.open_or_create_hash_table(path, backing_path, num_blocks, chain_length);
  uint64_t periodicity_ms_ = 1000;
  uint64_t put_tot_time = 0, put_t0 = 0, put_t1 = 0;

  std::atomic_bool stop_{false};
  std::size_t j = 0;
  auto worker_ = std::thread([&] {
    std::ofstream out("dataset.trace");
    while (!stop_.load()) {
      auto start = time_utils::now_ms();
      try {
        auto cur_epoch = time_utils::now_ms();
        out << cur_epoch;
        out << "\t" << j * 100 * 1024;
        out << std::endl;
      } catch (std::exception &e) {
        LOG(log_level::error) << "Exception: " << e.what();
      }
      auto end = time_utils::now_ms();
      auto elapsed = end - start;

      auto time_to_wait = periodicity_ms_ - elapsed;
      if (time_to_wait > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(time_to_wait));
      }
    }
    out.close();
  });
  std::ofstream out("latency.trace");
  for (j = 0; j < num_ops; ++j) {
    auto key = keys[j];
    std::string data_(102400 - key.size(), 'x');
    put_t0 = time_utils::now_us();
    ht_client->put(key, data_);
    put_t1 = time_utils::now_us();
    put_tot_time = (put_t1 - put_t0);
    auto cur_epoch = time_utils::now_ms();
    out << cur_epoch << " " << put_tot_time << " put" << key << std::endl;
  }

  uint64_t remove_tot_time = 0, remove_t0 = 0, remove_t1 = 0;
  for (j = num_ops - 1; j >= 0; --j) {
    auto key = keys[num_ops - 1 - j];
    remove_t0 = time_utils::now_us();
    ht_client->remove(key);
    remove_t1 = time_utils::now_us();
    remove_tot_time = (remove_t1 - remove_t0);
    auto cur_epoch = time_utils::now_ms();
    out << cur_epoch << " " << remove_tot_time << " remove " << key << std::endl;
    if (j == 0)
      break;
  }

  stop_.store(true);
  if (worker_.joinable())
    worker_.join();
  client.remove(path);
  return 0;
}
