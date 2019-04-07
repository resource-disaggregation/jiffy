#include <vector>
#include <thread>
#include <boost/program_options.hpp>
#include <jiffy/client/jiffy_client.h>
#include <jiffy/utils/logger.h>
#include <jiffy/utils/signal_handling.h>
#include <jiffy/utils/time_utils.h>
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
  std::string address = "127.0.0.1";
  int service_port = 9090;
  int lease_port = 9091;
  int num_blocks = 1;
  int chain_length = 1;
  // TODO change this to 64GB / 100KB each chunk
  size_t num_ops = 671088;
  //size_t num_ops = 3000;
  // TODO change this to 100KB, should have 64GB in total
  size_t data_size = 102400;
  std::string op_type = "msg_queue_auto_scaling";
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
  std::chrono::milliseconds periodicity_ms_(1000);
  std::atomic_bool stop_{false};
  std::size_t j = 0;
  auto worker_ = std::thread([&] {
    std::ofstream out("dataset.trace");
    while (!stop_.load()) {
      auto start = std::chrono::steady_clock::now();
      try {
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
  jiffy_client client(address, service_port, lease_port);
  std::shared_ptr<msg_queue_client>
      mq_client = client.open_or_create_msg_queue(path, backing_path, num_blocks, chain_length);
  std::string data_(data_size, 'x');
  uint64_t send_tot_time = 0, send_t0 = 0, send_t1 = 0;
  for (j = 0; j < num_ops; ++j) {
    send_t0 = time_utils::now_us();
    mq_client->send(data_);
    send_t1 = time_utils::now_us();
    send_tot_time = send_t1 - send_t0;
    auto cur_epoch = ts::duration_cast<ts::milliseconds>(ts::system_clock::now().time_since_epoch()).count();
    LOG(log_level::info) << "Latency for time: " << cur_epoch << " is " << send_tot_time << " us";
  }
  stop_.store(true);
  if (worker_.joinable())
    worker_.join();
  client.remove(path);
  return 0;
}
