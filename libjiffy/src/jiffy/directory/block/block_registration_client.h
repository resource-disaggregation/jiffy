#ifndef JIFFY_BLOCK_ALLOCATION_CLIENT_H
#define JIFFY_BLOCK_ALLOCATION_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "block_registration_service.h"

namespace jiffy {
namespace directory {
/* Block advertisement client class */
class block_registration_client {
 public:
  typedef block_registration_serviceClient thrift_client;

  block_registration_client() = default;

  /**
   * @brief Destructor
   */

  ~block_registration_client();

  /**
   * @brief Constructor
   * @param hostname Block allocation server hostname
   * @param port port number
   */

  block_registration_client(const std::string &hostname, int port);

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
   * @brief Register blocks with the directory server
   * @param block_names Block names
   */

  void register_blocks(const std::vector<std::string> &block_names);

  /**
   * @brief De-register blocks with the directory server
   * @param block_names Block names
   */

  void deregister_blocks(const std::vector<std::string> &block_names);

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

#endif //JIFFY_BLOCK_ALLOCATION_CLIENT_H
