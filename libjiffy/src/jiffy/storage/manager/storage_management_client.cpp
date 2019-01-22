#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "storage_management_client.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

namespace jiffy {
namespace storage {

storage_management_client::~storage_management_client() {
  if (transport_ != nullptr)
    disconnect();
}

storage_management_client::storage_management_client(const std::string &host, int port) {
  connect(host, port);
}

void storage_management_client::connect(const std::string &host, int port) {
  socket_ = std::make_shared<TSocket>(host, port);
  transport_ = std::shared_ptr<TTransport>(new TBufferedTransport(socket_));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
}

void storage_management_client::disconnect() {
  if (transport_->isOpen()) {
    transport_->close();
  }
  if (transport_->isOpen()) {
    transport_->close();
  }
}

void storage_management_client::create_partition(int32_t block_id,
                                                 const std::string &type,
                                                 const std::string &name,
                                                 const std::string &metadata,
                                                 const std::map<std::string, std::string> &conf) {
  client_->create_partition(block_id, type, name, metadata, conf);
}

void storage_management_client::setup_chain(int32_t block_id,
                                            const std::string &path,
                                            const std::vector<std::string> &chain,
                                            int32_t role,
                                            const std::string &next_block_id) {
  client_->setup_chain(block_id, path, chain, role, next_block_id);
}

void storage_management_client::destroy_partition(int32_t block_id) {
  client_->destroy_partition(block_id);
}

std::string storage_management_client::path(int32_t block_id) {
  std::string path;
  client_->get_path(path, block_id);
  return path;
}

void storage_management_client::sync(int32_t block_id, const std::string &backing_path) {
  client_->sync(block_id, backing_path);
}

void storage_management_client::load(int32_t block_id, const std::string &backing_path) {
  client_->load(block_id, backing_path);
}

void storage_management_client::dump(int32_t block_id, const std::string &backing_path) {
  client_->dump(block_id, backing_path);
}

int64_t storage_management_client::storage_capacity(const int32_t block_id) {
  return client_->storage_capacity(block_id);
}

int64_t storage_management_client::storage_size(const int32_t block_id) {
  return client_->storage_size(block_id);
}

void storage_management_client::resend_pending(int32_t block_id) {
  client_->resend_pending(block_id);
}

void storage_management_client::forward_all(int32_t block_id) {
  client_->forward_all(block_id);
}

}
}
