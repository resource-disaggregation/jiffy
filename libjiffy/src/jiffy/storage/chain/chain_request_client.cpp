#include "chain_request_client.h"

#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

namespace jiffy {
namespace storage {

chain_request_client::chain_request_client(const std::string &host, int32_t port, int32_t block_id) {
  connect(host, port, block_id);
}

chain_request_client::~chain_request_client() {
  disconnect();
}

void chain_request_client::connect(const std::string &host, int port, int32_t block_id) {
  host_ = host;
  port_ = port;
  block_id_ = block_id;
  socket_ = std::make_shared<TSocket>(host, port);
  transport_ = std::make_shared<TFramedTransport>(socket_);
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
}

void chain_request_client::disconnect() {
  if (is_connected()) {
    transport_->close();
  }
  block_id_ = -1;
}

bool chain_request_client::is_connected() const {
  return transport_ != nullptr && transport_->isOpen();
}

std::shared_ptr<apache::thrift::protocol::TProtocol> chain_request_client::protocol() const {
  return protocol_;
}

std::string chain_request_client::endpoint() {
  return host_ + ":" + std::to_string(port_);
}

void chain_request_client::request(const sequence_id &seq, const std::vector<std::string> &arguments) {
  client_->chain_request(seq, block_id_, arguments);
}

void chain_request_client::run_command(std::vector<std::string> &_return,
                                       const std::vector<std::string> &arguments) {
  client_->run_command(_return, block_id_, arguments);
}

}
}
