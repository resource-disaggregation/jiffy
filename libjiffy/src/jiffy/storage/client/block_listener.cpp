#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <iostream>

#include "jiffy/storage/client/block_listener.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::jiffy::utils;

block_listener::block_listener(const std::string &host,
                               int port,
                               mailbox_t &controls)
    : controls_(controls) {
  connect(host, port);
}

block_listener::~block_listener() {
  if (transport_ != nullptr)
    disconnect();
}

std::shared_ptr<apache::thrift::protocol::TProtocol> block_listener::protocol() {
  return protocol_;
}

void block_listener::connect(const std::string &host, int port) {
  socket_ = std::make_shared<TSocket>(host, port);
  transport_ = std::shared_ptr<TTransport>(new TFramedTransport(socket_));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
}

void block_listener::disconnect() {
  try {
    if (transport_->isOpen()) {
      transport_->close();
    }
  } catch (TTransportException &e) {
    LOG(log_level::warn) << "Could not close connection: " << e.what();
  }
}

void block_listener::subscribe(int32_t block_id, const std::vector<std::string> &ops) {
  client_->subscribe(block_id, ops);
  auto ret = controls_.pop(3000);
  if (ret.first == "error") {
    throw std::runtime_error(ret.second);
  }
}

void block_listener::unsubscribe(int32_t block_id, const std::vector<std::string> &ops) {
  client_->unsubscribe(block_id, ops);
  auto ret = controls_.pop(3000);
  if (ret.first == "error") {
    throw std::runtime_error(ret.second);
  }
}

}
}
