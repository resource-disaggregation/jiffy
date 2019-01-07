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

  /**
   * @brief Destructor
   */

  ~block_advertisement_client();

  /**
   * @brief Constructor
   * @param hostname Block allocation server hostname
   * @param port port number
   */

  block_advertisement_client(const std::string &hostname, int port);

  /**
   * @brief Connect Block allocation server
   * @param hostname Block allocation server hostname
   * @param port Port number
   */

  void connect(const std::string &hostname, int port);

  /**
   * @brief Disconnect server
   */

  void disconnect();

  /**
   * @brief Advertise all blocks to the directory server
   * @param block_names Block names
   */

  void advertise_blocks(const std::vector<std::string> &block_names);

  /**
   * @brief Remove blocks
   * @param block_names Block names
   */

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
