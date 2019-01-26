#include "block_registration_service_factory.h"
#include <thrift/transport/TSocket.h>
#include "block_registration_service_handler.h"
#include "../../utils/logger.h"

namespace jiffy {
namespace directory {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;
using namespace utils;

block_registration_service_factory::block_registration_service_factory(std::shared_ptr<block_allocator> alloc)
    : alloc_(std::move(alloc)) {}

block_registration_serviceIf *block_registration_service_factory::getHandler(const ::apache::thrift::TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(conn_info.transport);
  LOG(trace) << "Incoming connection from " << sock->getSocketInfo();
  return new block_registration_service_handler(alloc_);
}

void block_registration_service_factory::releaseHandler(block_registration_serviceIf *handler) {
  LOG(trace) << "Releasing connection...";
  delete handler;
}

}
}
