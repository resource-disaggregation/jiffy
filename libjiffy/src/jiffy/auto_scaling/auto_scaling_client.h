#ifndef JIFFY_AUTO_SCALING_RPC_CLIENT_H
#define JIFFY_AUTO_SCALING_RPC_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "auto_scaling_service.h"


namespace jiffy {
namespace auto_scaling {
/* Storage management client class */
class auto_scaling_client {
 public:
  typedef auto_scaling_serviceClient thrift_client;
  auto_scaling_client() = default;

  /**
   * @brief Destructor
   */

  ~auto_scaling_client();

  /**
   * @brief Constructor
   * @param host Storage management server hostname
   * @param port Port number
   */

  auto_scaling_client(const std::string &host, int port);

  /**
   * @brief Connect
   * @param host Storage management server hostname
   * @param port Port number
   */

  void connect(const std::string &host, int port);

  /**
   * @brief Disconnect
   */

  void disconnect();

  void auto_scaling(std::string& _return, const std::vector<std::string> & current_replica_chain, const std::string& path, const std::map<std::string, std::string> & conf);

 private:
  /* Socket */
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  /* Transport */
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  /* Protocol */
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  /* Storage management service client */
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //JIFFY_AUTO_SCALING_RPC_CLIENT_H
