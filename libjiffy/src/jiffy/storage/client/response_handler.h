#ifndef JIFFY_RESPONSE_HANDLER_H
#define JIFFY_RESPONSE_HANDLER_H

#include <string>
#include <atomic>
#include "jiffy/storage/service/block_response_service.h"
#include "jiffy/storage/notification/blocking_queue.h"

namespace jiffy {
namespace storage {

/* response handler class
 * Inherited from subscription_serviceIf */
class response_handler : public block_response_serviceIf {
 public:
  typedef blocking_queue<std::vector<std::string>> mailbox_t;
  /**
   * @brief Constructor
   * @param responses response mailbox
   * @param controls Control mailbox
   */

  explicit response_handler(std::vector<std::shared_ptr<mailbox_t>> &responses);
  void response(const sequence_id &seq, const std::vector<std::string> &result) override;
  void chain_ack(const sequence_id &seq) override;


  // Unsupported operations
  void notification(const std::string &op, const std::string &data) override;
  void control(response_type type, const std::vector<std::string> &ops, const std::string &msg) override;

 private:
  /* response mailbox
   * The response mailbox is like a response
   * buffer as to prevent client from being overwhelmed
   */
  std::vector<std::shared_ptr<mailbox_t>> responses_;
};

}
}










#endif //JIFFY_RESPONSE_HANDLER_H
