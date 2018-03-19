#include <thrift/transport/TSocket.h>

#include "kv_rpc_service_factory.h"
#include "kv_rpc_service_handler.h"
#include "../../utils/logger.h"

namespace elasticmem {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;
using namespace utils;

kv_rpc_service_factory::kv_rpc_service_factory(std::vector<std::shared_ptr<kv_block>> &blocks) : blocks_(blocks) {}

kv_rpc_serviceIf *kv_rpc_service_factory::getHandler(const TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(conn_info.transport);
  LOG(trace) << "Incoming connection from " << sock->getSocketInfo();
  return new kv_rpc_service_handler(blocks_);
}

void kv_rpc_service_factory::releaseHandler(kv_rpc_serviceIf *handler) {
  delete handler;
}

}
}
