#ifndef MMUX_NOTIFICATION_WORKER_H
#define MMUX_NOTIFICATION_WORKER_H

#include <thrift/protocol/TProtocol.h>
#include "notification_handler.h"
#include "subscription_service.h"

namespace mmux {
namespace storage {

class notification_worker {
 public:
  typedef subscription_serviceProcessor processor_t;
  typedef std::shared_ptr<processor_t> processor_ptr_t;
  typedef std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_ptr_t;
  typedef blocking_queue<std::pair<std::string, std::string>> mailbox_t;

  notification_worker(mailbox_t &notifications, mailbox_t &controls);
  ~notification_worker();

  void add_protocol(protocol_ptr_t protocol);

  void start();
  void stop();

 private:
  mailbox_t &notifications_;
  mailbox_t &controls_;
  std::atomic_bool stop_;
  std::thread worker_;

  processor_ptr_t processor_;
  std::vector<protocol_ptr_t> protocols_;
};

}
}

#endif //MMUX_NOTIFICATION_WORKER_H
