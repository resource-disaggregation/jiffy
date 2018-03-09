#include <iostream>

#include <thrift/transport/TSocket.h>
#include "directory_lease_service_factory.h"
#include "directory_lease_service_handler.h"

namespace elasticmem {
namespace directory {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;

directory_lease_service_factory::directory_lease_service_factory(std::shared_ptr<directory_service_shard> shard)
    : shard_(std::move(shard)) {}

directory_lease_serviceIf *directory_lease_service_factory::getHandler(const ::apache::thrift::TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(
      conn_info.transport);
  std::cerr << "Incoming connection\n"
            << "\t\t\tSocketInfo: " << sock->getSocketInfo() << "\n"
            << "\t\t\tPeerHost: " << sock->getPeerHost() << "\n"
            << "\t\t\tPeerAddress: " << sock->getPeerAddress() << "\n"
            << "\t\t\tPeerPort: " << sock->getPeerPort();
  return new directory_lease_service_handler(shard_);
}

void directory_lease_service_factory::releaseHandler(directory_lease_serviceIf *handler) {
  delete handler;
}

}
}