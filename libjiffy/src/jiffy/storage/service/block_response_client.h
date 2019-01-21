#ifndef JIFFY_BLOCK_RESPONSE_CLIENT_H
#define JIFFY_BLOCK_RESPONSE_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "block_response_service.h"

namespace jiffy {
namespace storage {
/* Block response client */
class block_response_client {
 public:
  typedef block_response_serviceClient thrift_client;

  /**
   * @brief Constructor
   * @param protocol Protocol
   */

  explicit block_response_client(std::shared_ptr<apache::thrift::protocol::TProtocol> protocol);

  /**
   * @brief Response
   * @param seq Sequence identifier
   * @param result Operation result
   */

  void response(const sequence_id &seq, const std::vector<std::string> &result);

 private:
  /* Block response service client */
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //JIFFY_BLOCK_RESPONSE_CLIENT_H
