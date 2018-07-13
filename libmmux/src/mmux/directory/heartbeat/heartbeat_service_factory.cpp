#include <thrift/transport/TSocket.h>
#include "heartbeat_service_factory.h"
#include "../../utils/logger.h"
#include "heartbeat_service_handler.h"

namespace mmux {
namespace directory {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;
using namespace utils;

heartbeat_service_factory::heartbeat_service_factory(std::shared_ptr<block_allocator> alloc,
                                                     std::shared_ptr<directory_management_ops> mgmt,
                                                     std::shared_ptr<std::map<std::string, health_metadata>> hm_map)
    : alloc_(alloc), mgmt_(mgmt), hm_map_(hm_map) {}

heartbeat_serviceIf *heartbeat_service_factory::getHandler(const ::apache::thrift::TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(conn_info.transport);
  LOG(trace) << "Incoming connection from " << sock->getSocketInfo();
  return new heartbeat_service_handler(alloc_, mgmt_, hm_map_);
}

void heartbeat_service_factory::releaseHandler(heartbeat_serviceIf *handler) {
  LOG(trace) << "Releasing connection";
  delete handler;
}

}
}
