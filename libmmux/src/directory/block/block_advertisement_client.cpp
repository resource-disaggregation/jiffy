#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "block_advertisement_client.h"

namespace mmux {
namespace directory {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

block_advertisement_client::~block_advertisement_client() {
  if (transport_ != nullptr)
    disconnect();
}

block_advertisement_client::block_advertisement_client(const std::string &host, int port) {
  connect(host, port);
}

void block_advertisement_client::connect(const std::string &host, int port) {
  socket_ = std::make_shared<TSocket>(host, port);
  transport_ = std::shared_ptr<TTransport>(new TBufferedTransport(socket_));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
}

void block_advertisement_client::disconnect() {
  if (transport_->isOpen()) {
    transport_->close();
  }
}

void block_advertisement_client::advertise_blocks(const std::vector<std::string> &block_names) {
  client_->add_blocks(block_names);
}

void block_advertisement_client::retract_blocks(const std::vector<std::string> &block_names) {
  client_->remove_blocks(block_names);
}

}
}