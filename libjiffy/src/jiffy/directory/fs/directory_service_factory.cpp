#include <iostream>

#include <thrift/transport/TSocket.h>
#include "directory_service_factory.h"
#include "directory_service_handler.h"
#include "../../utils/logger.h"

namespace jiffy {
namespace directory {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;
using namespace utils;

directory_service_factory::directory_service_factory(std::shared_ptr<directory_tree> shard)
    : shard_(std::move(shard)) {
}

directory_serviceIf *directory_service_factory::getHandler(const TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(conn_info.transport);
  LOG(trace) << "Incoming connection from " << sock->getSocketInfo();
  return new directory_service_handler(shard_);
}

void directory_service_factory::releaseHandler(directory_serviceIf *handler) {
  LOG(trace) << "Releasing connection";
  delete handler;
}

}
}
