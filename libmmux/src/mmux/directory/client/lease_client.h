#ifndef MMUX_DIRECTORY_LEASE_CLIENT_H
#define MMUX_DIRECTORY_LEASE_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "../lease/lease_service.h"

namespace mmux {
namespace directory {

class lease_client {
 public:
  typedef lease_serviceClient thrift_client;

  lease_client() = default;
  ~lease_client();
  lease_client(const std::string &hostname, int port);
  void connect(const std::string &hostname, int port);
  void disconnect();

  rpc_lease_ack renew_leases(const std::vector<std::string> &to_renew);

 private:
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //MMUX_DIRECTORY_LEASE_CLIENT_H
