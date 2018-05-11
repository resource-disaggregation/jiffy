#ifndef MMUX_BROKER_CLIENT_H
#define MMUX_BROKER_CLIENT_H

#include <string>
#include <queue>
#include <thread>
#include <thrift/transport/TSocket.h>
#include <thrift/protocol/TProtocol.h>
#include <condition_variable>
#include <atomic>
#include "notification_service.h"
#include "subscription_service.h"
#include "blocking_queue.h"

namespace mmux {
namespace storage {

class subscriber {
 public:
  typedef notification_serviceClient thrift_client;
  subscriber() = default;
  ~subscriber();
  subscriber(const std::string &host, int port);
  void connect(const std::string &host, int port);
  void disconnect();

  void subscribe(int32_t block_id, const std::vector<std::string> &ops);
  void unsubscribe(int32_t block_id, const std::vector<std::string> &ops);

  std::pair<std::string, std::string> get_message(int64_t timeout_ms = -1) {
    return mailbox_.pop(timeout_ms);
  }

 private:
  std::thread notification_worker_;
  blocking_queue<std::pair<std::string, std::string>> mailbox_;

  std::atomic_int response_{-1};
  std::condition_variable response_cond_;
  std::mutex response_mtx_;

  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  std::shared_ptr<thrift_client> client_{};
};

class notification_handler : public subscription_serviceIf {
 public:
  explicit notification_handler(blocking_queue<std::pair<std::string, std::string>> &mailbox,
                                std::atomic_int &response,
                                std::condition_variable &response_cond);
  void notification(const std::string &op, const std::string &data) override;
  void success(response_type type, const std::vector<std::string> &op) override;
  void error(response_type type, const std::string &op) override;

 private:
  blocking_queue<std::pair<std::string, std::string>> &mailbox_;
  std::atomic_int &response_;
  std::condition_variable &response_cond_;
};

}
}

#endif //MMUX_BROKER_CLIENT_H
