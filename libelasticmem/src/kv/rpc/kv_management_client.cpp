#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "kv_management_client.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

namespace elasticmem {
namespace kv {

kv_management_client::~kv_management_client() {
  if (transport_ != nullptr)
    disconnect();
}

kv_management_client::kv_management_client(const std::string &host, int port) {
  connect(host, port);
}

void kv_management_client::connect(const std::string &host, int port) {
  socket_ = std::make_shared<TSocket>(host, port);
  transport_ = std::shared_ptr<TTransport>(new TBufferedTransport(socket_));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
}

void kv_management_client::disconnect() {
  if (transport_->isOpen()) {
    transport_->close();
  }if (transport_->isOpen()) {
    transport_->close();
  }
}

void kv_management_client::flush(const int32_t block_id,
                                 const std::string &persistent_store_prefix,
                                 const std::string &path) {
  client_->flush(block_id, persistent_store_prefix, path);
}

void kv_management_client::load(const int32_t block_id,
                                const std::string &persistent_store_prefix,
                                const std::string &path) {
  client_->load(block_id, persistent_store_prefix, path);
}

void kv_management_client::clear(const int32_t block_id) {
  client_->clear(block_id);
}

int64_t kv_management_client::storage_capacity(const int32_t block_id) {
  return client_->storage_capacity(block_id);
}

int64_t kv_management_client::storage_size(const int32_t block_id) {
  return client_->storage_size(block_id);
}

}
}