#include <thrift/transport/TSocket.h>

#include "storage_management_service_factory.h"
#include "storage_management_service_handler.h"
#include "../../utils/logger.h"

namespace jiffy {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;
using namespace utils;

storage_management_service_factory::storage_management_service_factory(std::vector<std::shared_ptr<block>> &blocks)
    : blocks_(blocks) {}

storage_management_serviceIf *storage_management_service_factory::getHandler(const TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(conn_info.transport);
  LOG(trace) << "Incoming connection from " << sock->getSocketInfo();
  return new storage_management_service_handler(blocks_);
}

void storage_management_service_factory::releaseHandler(storage_management_serviceIf *handler) {
  LOG(trace) << "Releasing connection";
  delete handler;
}

}
}
