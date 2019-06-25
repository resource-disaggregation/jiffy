#include <thrift/transport/TBufferTransports.h>
#include <thrift/protocol/TBinaryProtocol.h>

#include "auto_scaling_client.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

namespace jiffy {
namespace auto_scaling {

auto_scaling_client::~auto_scaling_client() {
  if (transport_ != nullptr)
    disconnect();
}

auto_scaling_client::auto_scaling_client(const std::string &host, int port) {
  connect(host, port);
}

void auto_scaling_client::connect(const std::string &host, int port) {
  socket_ = std::make_shared<TSocket>(host, port);
  transport_ = std::shared_ptr<TTransport>(new TBufferedTransport(socket_));
  protocol_ = std::shared_ptr<TProtocol>(new TBinaryProtocol(transport_));
  client_ = std::make_shared<thrift_client>(protocol_);
  transport_->open();
}

void auto_scaling_client::disconnect() {
  if (transport_->isOpen()) {
    transport_->close();
  }
  if (transport_->isOpen()) {
    transport_->close();
  }
}

void auto_scaling_client::auto_scaling(const std::vector<std::string> &current_replica_chain,
                                       const std::string &path,
                                       const std::map<std::string, std::string> &conf) {
  client_->auto_scaling(current_replica_chain, path, conf);
}

}
}
