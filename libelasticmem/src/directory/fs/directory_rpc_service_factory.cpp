#include <iostream>

#include <thrift/transport/TSocket.h>
#include "directory_rpc_service_factory.h"
#include "directory_rpc_service_handler.h"
#include "../../utils/logger.h"

namespace elasticmem {
namespace directory {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;
using namespace utils;

directory_rpc_service_factory::directory_rpc_service_factory(std::shared_ptr<directory_tree> shard)
    : shard_(std::move(shard)) {
}

directory_rpc_serviceIf *directory_rpc_service_factory::getHandler(const TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(conn_info.transport);
  LOG(trace) <<  "Incoming connection from " << sock->getSocketInfo();
  return new directory_rpc_service_handler(shard_);
}

void directory_rpc_service_factory::releaseHandler(directory_rpc_serviceIf *handler) {
  LOG(trace) <<  "Releasing connection";
  delete handler;
}

}
}