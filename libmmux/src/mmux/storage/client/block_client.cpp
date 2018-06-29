#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <iostream>

#include "block_client.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

namespace mmux {
namespace storage {

block_client::~block_client() {
  if (transport_ != nullptr)
    disconnect();
}

int64_t block_client::get_client_id() {
  return client_->get_client_id();
}

void block_client::connect(const std::string &host, int port, int block_id, int timeout_ms) {
  block_id_ = block_id;
  auto sock = std::make_shared<TSocket>(host, port);
  sock->setRecvTimeout(timeout_ms);
  transport_ = std::shared_ptr<TTransport>(new TBufferedTransport(sock));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
}

block_client::command_response_reader block_client::get_command_response_reader(int64_t client_id) {
  client_->register_client_id(block_id_, client_id);
  return block_client::command_response_reader(protocol_);
}

void block_client::disconnect() {
  if (is_connected()) {
    transport_->close();
  }
  block_id_ = -1;
}

bool block_client::is_connected() const {
  if (transport_ == nullptr) return false;
  return transport_->isOpen();
}

void block_client::command_request(const sequence_id &seq, const int32_t cmd_id, const std::vector<std::string> &args) {
  client_->command_request(seq, block_id_, cmd_id, args);
}

}
}