#include <iostream>

#include <thrift/transport/TSocket.h>
#include "lease_service_factory.h"
#include "lease_service_handler.h"
#include "../../utils/logger.h"

namespace mmux {
namespace directory {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;
using namespace utils;

/**
 * Construction
 * @param tree directory tree
 * @param lease_period_ms lease duration
 */

lease_service_factory::lease_service_factory(std::shared_ptr<directory_tree> tree, int64_t lease_period_ms)
    : tree_(std::move(tree)), lease_period_ms_(lease_period_ms) {}

/**
 * Get lease_service_factory handler
 * @param conn_info connection information
 * @return handler
 */

lease_serviceIf *lease_service_factory::getHandler(const ::apache::thrift::TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(conn_info.transport);
  LOG(trace) << "Incoming connection from " << sock->getSocketInfo();
  return new lease_service_handler(tree_, lease_period_ms_);
}

/**
 * Release handler
 * @param handler handler
 */
void lease_service_factory::releaseHandler(lease_serviceIf *handler) {
  LOG(trace) << "Releasing connection";
  delete handler;
}

}
}