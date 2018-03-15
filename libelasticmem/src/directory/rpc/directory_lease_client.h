#ifndef ELASTICMEM_DIRECTORY_LEASE_CLIENT_H
#define ELASTICMEM_DIRECTORY_LEASE_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "directory_lease_service.h"

namespace elasticmem {
namespace directory {

class directory_lease_client {
 public:
  typedef directory_lease_serviceClient thrift_client;

  directory_lease_client() = default;
  ~directory_lease_client();
  directory_lease_client(const std::string &hostname, int port);
  void connect(const std::string &hostname, int port);
  void disconnect();

  rpc_lease_ack update_leases(const rpc_lease_update &updates);

 private:
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //ELASTICMEM_DIRECTORY_LEASE_CLIENT_H
