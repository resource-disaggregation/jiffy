#ifndef MMUX_NOTIFICATION_HANDLER_H
#define MMUX_NOTIFICATION_HANDLER_H

#include <string>
#include <atomic>
#include "subscription_service.h"
#include "blocking_queue.h"

namespace mmux {
namespace storage {

class notification_handler : public subscription_serviceIf {
 public:
  typedef blocking_queue<std::pair<std::string, std::string>> mailbox_t;

  explicit notification_handler(mailbox_t &notifications, mailbox_t& controls);
  void notification(const std::string &op, const std::string &data) override;
  void control(response_type type, const std::vector<std::string> &ops, const std::string &msg) override;

 private:
  mailbox_t &notifications_;
  mailbox_t &controls_;
};

}
}

#endif //MMUX_NOTIFICATION_HANDLER_H
