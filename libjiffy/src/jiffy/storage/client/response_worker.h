#ifndef JIFFY_RESPONSE_WORKER_CPP_H
#define JIFFY_RESPONSE_WORKER_CPP_H


#include <atomic>
#include <thrift/protocol/TProtocol.h>
#include "jiffy/storage/service/block_response_service.h"
#include "jiffy/storage/notification/blocking_queue.h"

namespace jiffy {
namespace storage {

/* response worker class */
class response_worker {
 public:
  typedef block_response_serviceProcessor processor_t;
  typedef std::shared_ptr<processor_t> processor_ptr_t;
  typedef std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_ptr_t;
  typedef blocking_queue<std::string> mailbox_t;

  /**
   * @brief Constructor
   * @param responses response mailbox
   * @param controls Control mailbox
   */

  response_worker(mailbox_t &responses);

  /**
   * @brief Destructor
   */

  ~response_worker();

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
  /* response mailbox */
  mailbox_t &responses_;
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





#endif //JIFFY_RESPONSE_WORKER_CPP_H
