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
using namespace ::jiffy::client;
using namespace ::jiffy::directory;
using namespace ::jiffy::storage;
using namespace ::jiffy::utils;
using namespace ::apache::thrift;
/*
std::vector<std::uint64_t> get_dist(double alpha, int max_val, int n) {
  std::vector<double> values;
  for (int value = 1; value <= max_val; value += 1)
    values.push_back(value);
  std::for_each(values.begin(), values.end(), [](double &n){ n = std::pow(n, -alpha);});
  std::vector<double> sum_;
  std::partial_sum(values.begin(), values.end(), sum_.begin(), std::plus<double>());
  auto back_value = sum_.back();
  sum_.insert(sum_.begin(), 0.0);
  std::for_each(sum_.begin(), sum_.end(), [](double &n){ n = n / back_value;});
  double rand_;
  srand(time(NULL));
  std::vector<std::uint64_t> ret;
  for(int i = 0;i < n; i++) {
    rand_= rand() % (1000)/float(1000);
    for(auto it = sum_.begin(); it != sum.end();it++) {
      if(*it <= rand)
        continue;
      else

    }
  }



  return values;
}
 */


std::vector<std::string> keygenerator(std::size_t num_keys, double theta, int num_buckets = 512) {
  int bucket_size = 65536 / num_buckets;
  hash_slot hashslot;
  zipfgenerator zipf(theta, num_keys);
  std::vector<std::uint64_t> bucket_dist;
  for(std::size_t i = 0; i < num_keys; i++) {
    bucket_dist.push_back(zipf.next());
  }
  int i = 0;
  std::vector<std::string> keys;
  while(keys.size() != num_keys) {
    char key[8];
    key[4] = 0x00;
    key[5] = 0x00;
    key[6] = 0x00;
    key[7] = 0x00;
    unsigned char* key_begin = reinterpret_cast<unsigned char*>(&i);
    for(int k = 0; k < 4; k++)
      key[k] = (char)*(key_begin + k);
    std::ostringstream convert;
    for(int j = 0; j < 8; j++) {
      convert << key[j];
    }
    std::string key_string = convert.str();
    auto bucket = std::uint64_t(hashslot.crc16(key, 8) / bucket_size);
    auto it = std::find (bucket_dist.begin(), bucket_dist.end(), bucket);
    if(it != bucket_dist.end()) {
      *it = *it - 1;
      keys.push_back(key_string);
      LOG(log_level::info) << "Found key" << keys.size();
      if (*it == 0) {
        it = bucket_dist.erase(it);
      }
    }
  i++;
  }
  return keys;
}

int main() {

  LOG(log_level::info) << "Look here!";
  std::vector<std::string> keys = keygenerator(671088, 1);

  std::string address = "127.0.0.1";
  int service_port = 9090;
  int lease_port = 9091;
  int num_blocks = 1;
  int chain_length = 1;
  // TODO change this to 64GB / 100KB each chunk
  size_t num_ops = 671088;
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
      mq_client = client.open_or_create_hash_table(path, backing_path, num_blocks, chain_length);
  std::string data_(data_size, 'x');
  double put_throughput_;
  double put_latency_;
  uint64_t put_tot_time = 0, put_t0 = 0, put_t1 = 0;
  auto put_bench_begin = time_utils::now_us();
  for (size_t j = 0; j < num_ops; ++j) {
    put_t0 = time_utils::now_us();
    mq_client->put(keys[j], data_);
    put_t1 = time_utils::now_us();
    put_tot_time += (put_t1 - put_t0);
  }
  put_latency_ = (double) put_tot_time / (double) num_ops;
  put_throughput_ = (double) num_ops * 1E6 / (double) (put_t1 - put_bench_begin);
  std::pair<double, double> put_result = std::make_pair(put_throughput_, put_latency_);
  double remove_throughput_;
  double remove_latency_;
  uint64_t remove_tot_time = 0, remove_t0 = 0, remove_t1 = 0;
  auto remove_bench_begin = time_utils::now_us();
  for (size_t j = 0; j < num_ops; ++j) {
    remove_t0 = time_utils::now_us();
    mq_client->remove(keys[j]);
    remove_t1 = time_utils::now_us();
    remove_tot_time += (remove_t1 - remove_t0);
  }
  remove_latency_ = (double) remove_tot_time / (double) num_ops;
  remove_throughput_ = (double) num_ops * 1E6 / (double) (remove_t1 - remove_bench_begin);
  std::pair<double, double> remove_result = std::make_pair(remove_throughput_, remove_latency_);


  client.remove(path);
  LOG(log_level::info) << "===== " << op_type << " ======";
  LOG(log_level::info) << "\t" << num_ops << " put requests completed in " << ((double) num_ops / put_result.first) << " us";
  LOG(log_level::info) << "\t" << data_size << " payload";
  LOG(log_level::info) << "\tAverage put latency: " << put_result.second;
  LOG(log_level::info) << "\tput throughput: " << put_result.first << " requests per microsecond";

  LOG(log_level::info) << "===== " << op_type << " ======";
  LOG(log_level::info) << "\t" << num_ops << " remove requests completed in " << ((double) num_ops / remove_result.first) << " us";
  LOG(log_level::info) << "\t" << data_size << " payload";
  LOG(log_level::info) << "\tAverage put latency: " << remove_result.second;
  LOG(log_level::info) << "\tput throughput: " << remove_result.first << " requests per microsecond";

  return 0;
}
