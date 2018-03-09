#include <iostream>

#include <thrift/transport/TSocket.h>
#include "directory_rpc_service_factory.h"
#include "directory_rpc_service_handler.h"

namespace elasticmem {
namespace directory {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;

directory_rpc_service_factory::directory_rpc_service_factory(std::shared_ptr<directory_service_shard> shard)
    : shard_(std::move(shard)) {
}

directory_rpc_serviceIf *directory_rpc_service_factory::getHandler(const TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(
      conn_info.transport);
  std::cerr << "Incoming connection\n"
            << "\t\t\tSocketInfo: " << sock->getSocketInfo() << "\n"
            << "\t\t\tPeerHost: " << sock->getPeerHost() << "\n"
            << "\t\t\tPeerAddress: " << sock->getPeerAddress() << "\n"
            << "\t\t\tPeerPort: " << sock->getPeerPort();
  return new directory_rpc_service_handler(shard_);
}

void directory_rpc_service_factory::releaseHandler(directory_rpc_serviceIf *handler) {
  delete handler;
}

}
}