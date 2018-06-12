#ifndef MMUX_BROKER_CLIENT_H
#define MMUX_BROKER_CLIENT_H

#include <atomic>
#include <queue>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransport.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/protocol/TMultiplexedProtocol.h>
#include "../notification/notification_service.h"
#include "../notification/subscription_service.h"
#include "../notification/blocking_queue.h"
#include "../notification/subscription_service.h"

namespace mmux {
namespace storage {

class block_listener {
 public:
  typedef notification_serviceClient thrift_client;

  typedef std::pair<std::string, std::string> notification_t;
  typedef blocking_queue<notification_t> mailbox_t;

  ~block_listener();
  block_listener(const std::string &host, int port, mailbox_t &notifications, mailbox_t &controls);

  void connect(const std::string &host, int port);
  void disconnect();

  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol();

  void subscribe(int32_t block_id, const std::vector<std::string> &ops);
  void unsubscribe(int32_t block_id, const std::vector<std::string> &ops);

 private:
  mailbox_t &notifications_;
  mailbox_t &controls_;

  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //MMUX_BROKER_CLIENT_H
