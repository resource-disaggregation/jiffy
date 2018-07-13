#ifndef MEMORYMUX_HEARTBEAT_SENDER_H
#define MEMORYMUX_HEARTBEAT_SENDER_H

#include <string>
#include <chrono>
#include <thread>
#include <atomic>
#include <thrift/transport/TSocket.h>
#include "../../directory/heartbeat/heartbeat_service.h"

namespace mmux {
namespace storage {

class heartbeat_sender {
 public:
  heartbeat_sender(const std::string sender_endpoint,
                   const std::string dir_host,
                   int heartbeat_port,
                   uint64_t heartbeat_period_ms);
  ~heartbeat_sender();

  void start();

  void stop();

 private:
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  std::shared_ptr<directory::heartbeat_serviceClient> client_{};

  std::chrono::milliseconds heartbeat_period_;
  std::thread worker_;
  std::atomic_bool stop_;
  directory::heartbeat hb_;
};

}
}

#endif //MEMORYMUX_HEARTBEAT_SENDER_H
