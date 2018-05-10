#include <thrift/protocol/TProtocol.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include "chain_request_handler_factory.h"

#include "../../utils/logger.h"
#include "chain_request_handler.h"

namespace mmux {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::protocol;
using namespace utils;

chain_request_handler_factory::chain_request_handler_factory(std::vector<std::shared_ptr<chain_module>> &blocks)
    : blocks_(blocks) {}

chain_request_serviceIf *chain_request_handler_factory::getHandler(const ::apache::thrift::TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(conn_info.transport);
  LOG(log_level::trace) << "Incoming connection from " << sock->getSocketInfo();
  std::shared_ptr<TTransport> transport(new TBufferedTransport(conn_info.transport));
  std::shared_ptr<TProtocol> prot(new TBinaryProtocol(transport));
  return new chain_request_handler(prot, blocks_);
}

void chain_request_handler_factory::releaseHandler(chain_request_serviceIf *handler) {
  LOG(log_level::trace) << "Releasing connection";
  delete handler;
}

}
}
