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

block_client::block_client(const std::string &host, int port, int block_id) {
  connect(host, port, block_id);
}

void block_client::connect(const std::string &host, int port, int block_id) {
  block_id_ = block_id;
  socket_ = std::make_shared<TSocket>(host, port);
  transport_ = std::shared_ptr<TTransport>(new TBufferedTransport(socket_));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
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

void block_client::run_command(std::vector<std::string> &_return, int op_id, const std::vector<std::string> &args) {
  client_->run_command(_return, block_id_, op_id, args);
}

void block_client::put(const key_type &key, const value_type &value) {
  std::vector<std::string> _ret;
  client_->run_command(_ret, block_id_, 1, {key, value});
}

value_type block_client::get(const key_type &key) {
  std::vector<std::string> _ret;
  client_->run_command(_ret, block_id_, 0, {key});
  return _ret.at(0);
}

void block_client::update(const key_type &key, const value_type &value) {
  std::vector<std::string> _ret;
  client_->run_command(_ret, block_id_, 3, {key, value});
}

void block_client::remove(const key_type &key) {
  std::vector<std::string> _ret;
  client_->run_command(_ret, block_id_, 2, {key});
}

}
}