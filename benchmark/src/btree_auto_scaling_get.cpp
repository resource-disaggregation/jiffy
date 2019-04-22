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
namespace ts = std::chrono;

int main() {
  std::string fileName = "../benchmark/src/zipf_random.txt";
  std::ifstream in(fileName.c_str());
  if (!in) {
    std::cerr << "Cannot open the File : " << fileName << std::endl;
  }
  //size_t num_ops = 419430;
  //size_t num_ops = 900;
  size_t num_ops = 70000;
  std::vector<std::string> keys;
  for (size_t j = 0; j < num_ops; j++) {
    std::string str;
    in >> str;
    if (str.size() > 0)
      keys.push_back(str);
  }
  in.close();

  std::string address = "127.0.0.1";
  int service_port = 9090;
  int lease_port = 9091;
  int num_blocks = 1;
  int chain_length = 1;
  std::string op_type = "btree_auto_scaling";
  std::string path = "/tmp";
  std::string backing_path = "local://tmp";

  // Output all the configuration parameters:
  LOG(log_level::info) << "host: " << address;
  LOG(log_level::info) << "service-port: " << service_port;
  LOG(log_level::info) << "lease-port: " << lease_port;
  LOG(log_level::info) << "num-blocks: " << num_blocks;
  LOG(log_level::info) << "chain-length: " << chain_length;
  LOG(log_level::info) << "num-ops: " << num_ops;
  //LOG(log_level::info) << "data-size: " << data_size;
  LOG(log_level::info) << "test: " << op_type;
  LOG(log_level::info) << "path: " << path;
  LOG(log_level::info) << "backing-path: " << backing_path;

  jiffy_client client(address, service_port, lease_port);
  std::shared_ptr<btree_client>
      bt_client = client.open_or_create_btree(path, backing_path, num_blocks, chain_length);
  uint64_t get_tot_time = 0, get_t0 = 0, get_t1 = 0;
  //std::chrono::milliseconds periodicity_ms_(1000);
  size_t k = 0;
  std::ofstream out("get_latency.trace");
  while (1) {
    try {
      for (k = 0; k < num_ops; k++) {
        auto key = keys[k];
        get_t0 = time_utils::now_us();
        bt_client->get(key);
        get_t1 = time_utils::now_us();
        get_tot_time = (get_t1 - get_t0);
        auto cur_epoch = ts::duration_cast<ts::milliseconds>(ts::system_clock::now().time_since_epoch()).count();
        out << cur_epoch << " " << get_tot_time << " get" << std::endl;
      }
    } catch (jiffy::directory::directory_service_exception &e) {
      break;
    }
  }
  return 0;
}
