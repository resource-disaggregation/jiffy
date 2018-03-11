#include <thrift/transport/TSocket.h>

#include "kv_rpc_service_factory.h"
#include "kv_rpc_service_handler.h"

namespace elasticmem {
namespace kv {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;

kv_rpc_service_factory::kv_rpc_service_factory(std::vector<std::shared_ptr<kv_block>> &blocks) : blocks_(blocks) {}

kv_rpc_serviceIf *kv_rpc_service_factory::getHandler(const TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(
      conn_info.transport);
  std::cerr << "Incoming connection\n"
            << "\t\t\tSocketInfo: " << sock->getSocketInfo() << "\n"
            << "\t\t\tPeerHost: " << sock->getPeerHost() << "\n"
            << "\t\t\tPeerAddress: " << sock->getPeerAddress() << "\n"
            << "\t\t\tPeerPort: " << sock->getPeerPort();
  return new kv_rpc_service_handler(blocks_);
}

void kv_rpc_service_factory::releaseHandler(kv_rpc_serviceIf *handler) {
  delete handler;
}

}
}
