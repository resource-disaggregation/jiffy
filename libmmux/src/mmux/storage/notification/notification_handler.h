#ifndef MMUX_NOTIFICATION_HANDLER_H
#define MMUX_NOTIFICATION_HANDLER_H

#include <string>
#include <atomic>
#include "subscription_service.h"
#include "blocking_queue.h"

namespace mmux {
namespace storage {

/* Notification handler class
 * Inherited from subscription_serviceIf */
class notification_handler : public subscription_serviceIf {
 public:
  typedef blocking_queue<std::pair<std::string, std::string>> mailbox_t;

  /**
   * @brief Constructor
   *
   * The notification mailbox is like a notification
   * buffer as to prevent client from being overwhelmed
   *
   * The control mailbox is a log for subscribe and
   * unsubscribe control operations
   *
   * @param notifications Notification mailbox
   * @param controls Control mailbox
   */

  explicit notification_handler(mailbox_t &notifications, mailbox_t &controls);

  /**
   * @brief Add notification to notification mailbox
   * @param op Operation
   * @param data Data
   */

  void notification(const std::string &op, const std::string &data) override;

  /**
   * @brief Add control operation to control mailbox
   * @param type response type,  subscribe or unsubscribe
   * @param ops Operations
   * @param msg message to be returned
   */

  void control(response_type type, const std::vector<std::string> &ops, const std::string &msg) override;

 private:
  /* Notification mailbox
   * The notification mailbox is like a notification
   * buffer as to prevent client from being overwhelmed
   */
  mailbox_t &notifications_;
  /* Control mailbox
   * The control mailbox is a log for subscribe and
   * unsubscribe control operations
   */
  mailbox_t &controls_;
};

}
}

#endif //MMUX_NOTIFICATION_HANDLER_H