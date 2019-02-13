#ifndef JIFFY_NOTIFICATION_WORKER_H
#define JIFFY_NOTIFICATION_WORKER_H

#include <atomic>
#include <thrift/protocol/TProtocol.h>
#include "jiffy/storage/service/block_response_service.h"
#include "blocking_queue.h"

namespace jiffy {
namespace storage {

/* Notification worker class */
class notification_worker {
 public:
  typedef block_response_serviceProcessor processor_t;
  typedef std::shared_ptr<processor_t> processor_ptr_t;
  typedef std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_ptr_t;
  typedef blocking_queue<std::pair<std::string, std::string>> mailbox_t;

  /**
   * @brief Constructor
   * @param notifications Notification mailbox
   * @param controls Control mailbox
   */

  notification_worker(mailbox_t &notifications, mailbox_t &controls);

  /**
   * @brief Destructor
   */

  ~notification_worker();

  /**
   * @brief Add protocol to protocol list
   * @param protocol
   */

  void add_protocol(protocol_ptr_t protocol);

  /**
   * @brief Start processor thread
   */

  void start();

  /**
   * @brief Stop worker
   */

  void stop();

 private:
  /* Notification mailbox */
  mailbox_t &notifications_;
  /* Control mailbox */
  mailbox_t &controls_;
  /* Atomic boolean stop */
  std::atomic_bool stop_;
  /* Worker thread */
  std::thread worker_;
  /* Processor */
  processor_ptr_t processor_;
  /* Protocols */
  std::vector<protocol_ptr_t> protocols_;
};

}
}

#endif //JIFFY_NOTIFICATION_WORKER_H
