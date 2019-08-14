#ifndef JIFFY_NOTIFICATION_RESPONSE_CLIENT_H
#define JIFFY_NOTIFICATION_RESPONSE_CLIENT_H

#include <jiffy/storage/service/block_response_service.h>

namespace jiffy {
namespace storage {

class notification_response_client {
 public:
  notification_response_client(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot);

  /**
   * @brief Send notification
   * @param op Operation
   * @param data Data
   */
  void notification(const std::string &op, const std::string &data);

  /**
   * @brief Send control message
   * @param type response type
   * @param ops Operations
   * @param msg Message
   */
  void control(response_type type, const std::vector<std::string> &ops, const std::string &msg);
 private:
  block_response_serviceClient client_;
};

}
}

#endif //JIFFY_NOTIFICATION_RESPONSE_CLIENT_H
