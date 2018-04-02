#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "block_client.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

namespace elasticmem {
namespace storage {

block_client::~block_client() {
  if (transport_ != nullptr)
    disconnect();
}

int64_t block_client::get_client_id() {
  return client_->get_client_id();
}

void block_client::connect(const std::string &host, int port, int block_id) {
  host_ = host;
  port_ = port;
  block_id_ = block_id;
  socket_ = std::make_shared<TSocket>(host, port);
  transport_ = std::shared_ptr<TTransport>(new TBufferedTransport(socket_));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
}

std::thread block_client::add_response_listener(int64_t client_id, promise_map &promises) {
  client_->register_client_id(block_id_, client_id);
  auto handler = std::make_shared<command_response_handler>(promises);
  auto processor = std::make_shared<block_response_serviceProcessor>(handler);
  return std::thread([=] {
    while (true) {
      try {
        if (!processor->process(protocol_, protocol_, nullptr)) {
          break;
        }
      } catch (std::exception &e) {
        break;
      }
    }
  });
}

void block_client::disconnect() {
  if (is_connected()) {
    transport_->close();
  }
  block_id_ = -1;
}

bool block_client::is_connected() {
  if (transport_ == nullptr) return false;
  return transport_->isOpen();
}

void block_client::command_request(const sequence_id &seq,
                                   const int32_t cmd_id,
                                   const std::vector<std::string> &arguments) {
  client_->command_request(seq, block_id_, cmd_id, arguments);
}

}
}