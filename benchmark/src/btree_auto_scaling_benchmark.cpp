#include <vector>
#include <thread>
#include <boost/program_options.hpp>
#include <jiffy/client/jiffy_client.h>
#include <jiffy/utils/logger.h>
#include <jiffy/utils/signal_handling.h>
#include <jiffy/utils/time_utils.h>
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
  std::vector<std::string> keys;
  std::string fileName = "../benchmark/src/random_keys.txt";
  std::ifstream in(fileName.c_str());
  // Check if object is valid
  if (!in) {
    std::cerr << "Cannot open the File : " << fileName << std::endl;
  }
  std::string str;
  // Read the next line from File until it reaches the end.
  while (in >> str) {
    if (str.size() > 0)
      keys.push_back(str);
  }
  //Close The File
  in.close();
  std::cout << "read key success " << keys.size() << std::endl;
  size_t num_ops = 440000;
  //size_t num_ops = 4000;
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
  std::chrono::milliseconds periodicity_ms_(1000);
  uint64_t put_tot_time = 0, put_t0 = 0, put_t1 = 0;

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
        out << "\t" << j * 100 * 1024;
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
  std::ofstream out("latency.trace");
  for (j = 0; j < num_ops; ++j) {
    auto key = keys[j];
    std::string data_(102400 - key.size(), 'x');
    put_t0 = time_utils::now_us();
    bt_client->put(key, data_);
    put_t1 = time_utils::now_us();
    put_tot_time = (put_t1 - put_t0);
    auto cur_epoch = ts::duration_cast<ts::milliseconds>(ts::system_clock::now().time_since_epoch()).count();
    out << cur_epoch << " " << put_tot_time << " put" << std::endl;
  }

  uint64_t remove_tot_time = 0, remove_t0 = 0, remove_t1 = 0;
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  for (j = num_ops - 1; j >= 0; --j) {
    auto key = keys[num_ops - 1 - j];
    remove_t0 = time_utils::now_us();
    bt_client->remove(key);
    remove_t1 = time_utils::now_us();
    remove_tot_time = (remove_t1 - remove_t0);
    auto cur_epoch = ts::duration_cast<ts::milliseconds>(ts::system_clock::now().time_since_epoch()).count();
    out << cur_epoch << " " << remove_tot_time << " remove" << std::endl;
    if(j == 0)
      break;
  }
  stop_.store(true);
  if (worker_.joinable())
    worker_.join();
  client.remove(path);
  return 0;
}
