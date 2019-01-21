#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "lease_client.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

namespace jiffy {
namespace directory {

lease_client::~lease_client() {
  if (transport_ != nullptr)
    disconnect();
}

lease_client::lease_client(const std::string &host, int port) {
  connect(host, port);
}

void lease_client::connect(const std::string &host, int port) {
  socket_ = std::make_shared<TSocket>(host, port);
  transport_ = std::shared_ptr<TTransport>(new TBufferedTransport(socket_));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
}

void lease_client::disconnect() {
  if (transport_->isOpen()) {
    transport_->close();
  }
}

rpc_lease_ack lease_client::renew_leases(const std::vector<std::string> &to_renew) {
  rpc_lease_ack ack;
  client_->renew_leases(ack, to_renew);
  return ack;
}

}
}
