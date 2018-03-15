#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "kv_client.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

namespace elasticmem {
namespace storage {

kv_client::~kv_client() {
  if (transport_ != nullptr)
    disconnect();
}

kv_client::kv_client(const std::string &host, int port, int block_id) : block_id_(block_id) {
  connect(host, port);
}

void kv_client::connect(const std::string &host, int port) {
  socket_ = std::make_shared<TSocket>(host, port);
  transport_ = std::shared_ptr<TTransport>(new TBufferedTransport(socket_));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
}

void kv_client::disconnect() {
  if (transport_->isOpen()) {
    transport_->close();
  }
}

void kv_client::put(const key_type &key, const value_type &value) {
  client_->put(block_id_, key, value);
}

value_type kv_client::get(const key_type &key) {
  value_type value;
  client_->get(value, block_id_, key);
  return value;
}

void kv_client::update(const key_type &key, const value_type &value) {
  client_->update(block_id_, key, value);
}

void kv_client::remove(const key_type &key) {
  client_->remove(block_id_, key);
}

std::size_t kv_client::size() const {
  return static_cast<size_t>(client_->size(block_id_));
}

bool kv_client::empty() const {
  return size() == 0;
}

}
}