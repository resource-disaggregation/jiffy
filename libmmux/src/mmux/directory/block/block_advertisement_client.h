#ifndef MMUX_BLOCK_ALLOCATION_CLIENT_H
#define MMUX_BLOCK_ALLOCATION_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "block_allocation_service.h"

namespace mmux {
namespace directory {
/* Block advertisement client class */
class block_advertisement_client {
 public:
  typedef block_allocation_serviceClient thrift_client;

  block_advertisement_client() = default;
  ~block_advertisement_client();
  block_advertisement_client(const std::string &hostname, int port);
  void connect(const std::string &hostname, int port);
  void disconnect();

  void advertise_blocks(const std::vector<std::string> &block_names);
  void retract_blocks(const std::vector<std::string> &block_names);

 private:
  /* Socket */
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  /* Transport */
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  /* Protocol */
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  /* Client */
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //MMUX_BLOCK_ALLOCATION_CLIENT_H
