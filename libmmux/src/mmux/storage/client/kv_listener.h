#ifndef MMUX_KV_LISTENER_H
#define MMUX_KV_LISTENER_H

#include "../../directory/directory_ops.h"
#include "block_listener.h"
#include "../notification/notification_worker.h"

namespace mmux {
namespace storage {
/* Key value block listener */
class kv_listener {
 public:
  typedef std::pair<std::string, std::string> notification_t;
  typedef blocking_queue<notification_t> mailbox_t;

  /**
   * @brief Constructor
   * @param path key value block path
   * @param status Data status
   */

  kv_listener(const std::string &path, const directory::data_status &status);

  /**
   * @brief Destructor
   * Close all block listeners
   */

  ~kv_listener();

  /**
   * @brief Subscribe for block on operations
   * @param ops operations
   */

  void subscribe(const std::vector<std::string> &ops);

  /**
   * @brief Unsubscribe for block on operations
   * @param ops operations
   */

  void unsubscribe(const std::vector<std::string> &ops);

  /**
   * @brief Get notification
   * @param timeout_ms timeout
   * @return Notification pair
   */

  notification_t get_notification(int64_t timeout_ms = -1);

 private:

  /* Notification mailbox
   * The notification mailbox is like a notification
   * buffer as to prevent client from being overwhelmed
   */

  mailbox_t notifications_;

  /* Control mailbox
   * The control mailbox is a log for subscribe and
   * unsubscribe control operations
   */

  mailbox_t controls_;

  /* Key value block path */
  std::string path_;
  /* Data status */
  directory::data_status status_;
  /* Notification worker */
  notification_worker worker_;
  /* Block listeners */
  std::vector<std::shared_ptr<block_listener>> listeners_;
  /* Block identifiers */
  std::vector<int32_t> block_ids_;
};

}
}

#endif //MMUX_KV_LISTENER_H