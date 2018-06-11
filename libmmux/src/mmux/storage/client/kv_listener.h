#ifndef MMUX_KV_LISTENER_H
#define MMUX_KV_LISTENER_H

#include "../../directory/directory_ops.h"
#include "block_listener.h"
#include "../notification/notification_worker.h"

namespace mmux {
namespace storage {

class kv_listener {
 public:
  typedef std::pair<std::string, std::string> notification_t;
  typedef blocking_queue<notification_t> mailbox_t;

  kv_listener(const std::string &path, const directory::data_status &status);
  ~kv_listener();

  void subscribe(const std::vector<std::string> &ops);
  void unsubscribe(const std::vector<std::string> &ops);
  notification_t get_notification(int64_t timeout_ms = -1);

 private:
  mailbox_t notifications_;
  mailbox_t controls_;

  std::string path_;
  directory::data_status status_;
  notification_worker worker_;

  std::vector<std::shared_ptr<block_listener>> listeners_;
  std::vector<int32_t> block_ids_;
};

}
}

#endif //MMUX_KV_LISTENER_H
