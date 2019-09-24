#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include "block_request_handler_factory.h"

#include "../../utils/logger.h"
#include "block_request_handler.h"

namespace jiffy {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace utils;

block_request_handler_factory::block_request_handler_factory(std::vector<std::shared_ptr<block>> &blocks)
    : client_id_gen_(1) {
  for (const auto &x : blocks) {
    auto bid = block_id_parser::parse(x->id());
    blocks_.emplace(std::make_pair(bid.id, x));
  }
}

block_request_serviceIf *block_request_handler_factory::getHandler(const ::apache::thrift::TConnectionInfo &conn_info) {
  std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(conn_info.transport);
  LOG(log_level::trace) << "Incoming connection from " << sock->getSocketInfo();
  auto transport = std::make_shared<TFramedTransport>(conn_info.transport);
  std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  return new block_request_handler(protocol, client_id_gen_, blocks_);
}

void block_request_handler_factory::releaseHandler(block_request_serviceIf *handler) {
  LOG(log_level::trace) << "Releasing connection";
  auto br_handler = reinterpret_cast<block_request_handler *>(handler);
  int64_t client_id = br_handler->registered_client_id();
  int32_t block_id = br_handler->registered_block_id();
  if (client_id != -1 && block_id != -1 && blocks_[static_cast<std::size_t>(block_id)]->valid()) {
    blocks_[static_cast<std::size_t>(block_id)]->impl()->clients().remove_client(client_id);
  }
  delete handler;
}

}
}
