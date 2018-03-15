#include <iostream>

#include <thrift/transport/TSocket.h>
#include "directory_lease_service_factory.h"
#include "directory_lease_service_handler.h"
#include "../../utils/logger.h"

namespace elasticmem {
namespace directory {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;
using namespace utils;

directory_lease_service_factory::directory_lease_service_factory(std::shared_ptr<directory_tree> tree,
                                                                 std::shared_ptr<kv::kv_management_service> kv)
    : tree_(std::move(tree)), kv_(std::move(kv)) {}

directory_lease_serviceIf *directory_lease_service_factory::getHandler(const ::apache::thrift::TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(
      conn_info.transport);
  LOG(trace) <<  "Incoming connection from " << sock->getSocketInfo();
  return new directory_lease_service_handler(tree_, kv_);
}

void directory_lease_service_factory::releaseHandler(directory_lease_serviceIf *handler) {
  delete handler;
}

}
}