#ifndef JIFFY_DIRECTORY_LEASE_CLIENT_H
#define JIFFY_DIRECTORY_LEASE_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "../lease/lease_service.h"

namespace jiffy {
namespace directory {

/* Lease client class */

class lease_client {
 public:
  typedef lease_serviceClient thrift_client;

  lease_client() = default;

  /**
   * @brief Destructor
   */

  ~lease_client();

  /**
   * @brief Constructor
   * @param host Lease server hostname
   * @param port Port number
   */

  lease_client(const std::string &hostname, int port);

  /**
   * @brief Connect lease server
   * @param host Lease server hostname
   * @param port Port number
   */

  void connect(const std::string &hostname, int port);

  /**
   * @brief Disconnect server
   */

  void disconnect();

  /**
   * @brief Renew lease
   * @param to_renew File to be renewed
   * @return Lease acknowledgement
   */

  rpc_lease_ack renew_leases(const std::vector<std::string> &to_renew);

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

#endif //JIFFY_DIRECTORY_LEASE_CLIENT_H
