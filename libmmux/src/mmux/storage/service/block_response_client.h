#ifndef MMUX_BLOCK_RESPONSE_CLIENT_H
#define MMUX_BLOCK_RESPONSE_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "block_response_service.h"

namespace mmux {
namespace storage {

class block_response_client {
 public:
  typedef block_response_serviceClient thrift_client;
  explicit block_response_client(std::shared_ptr<apache::thrift::protocol::TProtocol> protocol);

  void response(const sequence_id& seq, const std::vector<std::string>& result);
 private:
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //MMUX_BLOCK_RESPONSE_CLIENT_H
