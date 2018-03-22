#include <thrift/transport/TSocket.h>

#include "block_service_factory.h"
#include "block_service_handler.h"
#include "../../utils/logger.h"

namespace elasticmem {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;
using namespace utils;

block_service_factory::block_service_factory(std::vector<std::shared_ptr<kv_block>> &blocks,
                                               std::vector<std::shared_ptr<subscription_map>> &sub_maps)
    : blocks_(blocks), sub_maps_(sub_maps) {}

block_serviceIf *block_service_factory::getHandler(const TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(conn_info.transport);
  LOG(trace) << "Incoming connection from " << sock->getSocketInfo();
  return new block_service_handler(blocks_, sub_maps_);
}

void block_service_factory::releaseHandler(block_serviceIf *handler) {
  LOG(trace) << "Releasing connection";
  delete handler;
}

}
}
