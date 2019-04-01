#include <thrift/transport/TSocket.h>

#include "auto_scaling_service_factory.h"
#include "auto_scaling_service_handler.h"
#include "../../utils/logger.h"

namespace jiffy {
namespace auto_scaling {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;
using namespace utils;

auto_scaling_service_factory::auto_scaling_service_factory(std::vector<std::shared_ptr<block>> &blocks)
    : blocks_(blocks) {}

auto_scaling_serviceIf *auto_scaling_service_factory::getHandler(const TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(conn_info.transport);
  LOG(trace) << "Incoming connection from " << sock->getSocketInfo();
  return new auto_scaling_service_handler(blocks_);
}

void auto_scaling_service_factory::releaseHandler(auto_scaling_serviceIf *handler) {
  LOG(trace) << "Releasing connection";
  delete handler;
}

}
}
