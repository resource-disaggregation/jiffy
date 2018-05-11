#include "block_allocation_service_factory.h"
#include <thrift/transport/TSocket.h>
#include "block_allocation_service_handler.h"
#include "../../utils/logger.h"

namespace mmux {
namespace directory {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;
using namespace utils;

block_allocation_service_factory::block_allocation_service_factory(std::shared_ptr<block_allocator> alloc)
    : alloc_(std::move(alloc)) {}

block_allocation_serviceIf *block_allocation_service_factory::getHandler(const ::apache::thrift::TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(conn_info.transport);
  LOG(trace) << "Incoming connection from " << sock->getSocketInfo();
  return new block_allocation_service_handler(alloc_);
}

void block_allocation_service_factory::releaseHandler(block_allocation_serviceIf *handler) {
  LOG(trace) << "Releasing connection...";
  delete handler;
}

}
}
