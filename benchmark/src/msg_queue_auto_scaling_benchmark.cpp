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

int main() {
  std::string address = "127.0.0.1";
  int service_port = 9090;
  int lease_port = 9091;
  int num_blocks = 1;
  int chain_length = 1;
  // TODO change this to 64GB / 100KB each chunk
  size_t num_ops = 671088;
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

  jiffy_client client(address, service_port, lease_port);
  std::shared_ptr<msg_queue_client>
      mq_client = client.open_or_create_msg_queue(path, backing_path, num_blocks, chain_length);
  std::string data_(data_size, 'x');
  double send_throughput_;
  double send_latency_;
  uint64_t send_tot_time = 0, send_t0 = 0, send_t1 = 0;
  auto send_bench_begin = time_utils::now_us();
  for (size_t j = 0; j < num_ops; ++j) {
    send_t0 = time_utils::now_us();
    mq_client->send(data_);
    send_t1 = time_utils::now_us();
    send_tot_time += (send_t1 - send_t0);
  }
  send_latency_ = (double) send_tot_time / (double) num_ops;
  send_throughput_ = (double) num_ops * 1E6 / (double) (send_t1 - send_bench_begin);
  std::pair<double, double> send_result = std::make_pair(send_throughput_, send_latency_);
  client.remove(path);
  LOG(log_level::info) << "===== " << op_type << " ======";
  LOG(log_level::info) << "\t" << num_ops << " send requests completed in " << ((double) num_ops / send_result.first) << " us";
  LOG(log_level::info) << "\t" << data_size << " payload";
  LOG(log_level::info) << "\tAverage send latency: " << send_result.second;
  LOG(log_level::info) << "\tSend throughput: " << send_result.first << " requests per microsecond";
  return 0;
}
