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

std::vector<std::string> keygenerator(std::size_t num_keys, double theta, int num_buckets = 512) {
  int bucket_size = 65536 / num_buckets;
  hash_slot hashslot;
  zipfgenerator zipf(theta, num_keys);
  std::vector<std::uint64_t> bucket_dist;
  for (std::size_t i = 0; i < num_keys; i++) {
    bucket_dist.push_back(zipf.next());
  }
  std::uint64_t count = 1;
  std::vector<std::string> keys;
  while (keys.size() != num_keys) {
    char key[8];
    memcpy((char *) key, (char *) &count, 8);
    std::string key_string = std::string(8, '1');
    for (int p = 0; p < 8; p++) {
      key_string[p] = key[p];
      if ((int) ((std::uint8_t) key[p]) == 0)
        key_string[p] = 1;
    }
    auto bucket = std::uint64_t(hashslot.crc16(key, 8) / bucket_size);
    auto it = std::find(bucket_dist.begin(), bucket_dist.end(), bucket);
    if (it != bucket_dist.end()) {
      *it = *it - 1;
      keys.push_back(key_string);
      LOG(log_level::info) << "Found key" << keys.size();
      if (*it == 0) {
        it = bucket_dist.erase(it);
      }
    }
    count++;
  }
  return keys;
}

int main() {
  size_t num_ops = 1501;
  //size_t num_ops = 100;
  //std::vector<std::string> keys = keygenerator(num_ops, 0.5);

  std::string address = "127.0.0.1";
  int service_port = 9090;
  int lease_port = 9091;
  int num_blocks = 1;
  int chain_length = 1;
  // TODO change this to 64GB / 100KB each chunk
  //size_t num_ops = 671088;

  // TODO change this to 100KB, should have 64GB in total
  size_t data_size = 102392;
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
  LOG(log_level::info) << "data-size: " << data_size;
  LOG(log_level::info) << "test: " << op_type;
  LOG(log_level::info) << "path: " << path;
  LOG(log_level::info) << "backing-path: " << backing_path;

  jiffy_client client(address, service_port, lease_port);
  std::shared_ptr<hash_table_client>
      ht_client = client.open_or_create_hash_table(path, backing_path, num_blocks, chain_length);
  std::string data_(data_size, 'x');
  double put_throughput_;
  double put_latency_;
  std::chrono::milliseconds periodicity_ms_(1000);
  uint64_t put_tot_time = 0, put_t0 = 0, put_t1 = 0;
  /* Atomic stop bool */

  std::atomic_bool stop_{false};
  std::size_t j = 0;
  auto worker_ = std::thread([&] {
    std::ofstream out("dataset.trace");
    while (!stop_.load()) {
      auto start = std::chrono::steady_clock::now();
      try {
        namespace ts = std::chrono;
        auto cur_epoch = ts::duration_cast<ts::milliseconds>(ts::system_clock::now().time_since_epoch()).count();
        out << cur_epoch;
        out << "\t" << j * 100; // KB
        out << std::endl;
      } catch (std::exception &e) {
        LOG(log_level::error) << "Exception: " << e.what();
      }
      auto end = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

      auto time_to_wait = std::chrono::duration_cast<std::chrono::milliseconds>(periodicity_ms_ - elapsed);
      if (time_to_wait > std::chrono::milliseconds::zero()) {
        std::this_thread::sleep_for(time_to_wait);
      }
    }
    out.close();
  });
  auto put_bench_begin = time_utils::now_us();
  for (j = 0; j < num_ops; ++j) {
    put_t0 = time_utils::now_us();
    //ht_client->put(keys[j], data_);
    ht_client->put(std::to_string(j), data_);
    put_t1 = time_utils::now_us();
    put_tot_time += (put_t1 - put_t0);
  }
  put_latency_ = (double) put_tot_time / (double) num_ops;
  put_throughput_ = (double) num_ops * 1E6 / (double) (put_t1 - put_bench_begin);
  std::pair<double, double> put_result = std::make_pair(put_throughput_, put_latency_);
  double remove_throughput_;
  double remove_latency_;
  uint64_t remove_tot_time = 0, remove_t0 = 0, remove_t1 = 0;
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  auto remove_bench_begin = time_utils::now_us();
  for (j = 0; j < num_ops; ++j) {
    remove_t0 = time_utils::now_us();
    //ht_client->remove(keys[j]);
    ht_client->remove(std::to_string(j));
    remove_t1 = time_utils::now_us();
    remove_tot_time += (remove_t1 - remove_t0);
  }
  remove_latency_ = (double) remove_tot_time / (double) num_ops;
  remove_throughput_ = (double) num_ops * 1E6 / (double) (remove_t1 - remove_bench_begin);
  std::pair<double, double> remove_result = std::make_pair(remove_throughput_, remove_latency_);
  stop_.store(true);
  if (worker_.joinable())
    worker_.join();
  client.remove(path);
  LOG(log_level::info) << "===== " << op_type << " ======";
  LOG(log_level::info) << "\t" << num_ops << " put requests completed in " << ((double) num_ops / put_result.first)
                       << " us";
  LOG(log_level::info) << "\t" << data_size << " payload";
  LOG(log_level::info) << "\tAverage put latency: " << put_result.second;
  LOG(log_level::info) << "\tput throughput: " << put_result.first << " requests per microsecond";

  LOG(log_level::info) << "===== " << op_type << " ======";
  LOG(log_level::info) << "\t" << num_ops << " remove requests completed in "
                       << ((double) num_ops / remove_result.first) << " us";
  LOG(log_level::info) << "\t" << data_size << " payload";
  LOG(log_level::info) << "\tAverage put latency: " << remove_result.second;
  LOG(log_level::info) << "\tremove throughput: " << remove_result.first << " requests per microsecond";

  return 0;
}
