#include "connection_utils.h"

#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

int is_server_alive(const char *host, int port) {
  using namespace apache::thrift::transport;
  try {
    auto transport = std::shared_ptr<TTransport>(new TFramedTransport(std::make_shared<TSocket>(host, port)));
    transport->open();
    transport->close();
    return 1;
  } catch (TTransportException &e) {
    return 0;
  }
}
