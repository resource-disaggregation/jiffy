#include "lease_server.h"
#include "lease_service_factory.h"

#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>

namespace jiffy {
namespace directory {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

std::shared_ptr<TThreadedServer> lease_server::create(std::shared_ptr<directory_tree> tree, int64_t lease_period_ms,
                                                      const std::string &address, int port) {
  std::shared_ptr<lease_serviceIfFactory>
      clone_factory(new lease_service_factory(std::move(tree), lease_period_ms));
  std::shared_ptr<lease_serviceProcessorFactory>
      proc_factory(new lease_serviceProcessorFactory(clone_factory));
  std::shared_ptr<TServerSocket> sock(new TServerSocket(address, port));
  std::shared_ptr<TBufferedTransportFactory> transport_factory(new TBufferedTransportFactory());
  std::shared_ptr<TBinaryProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  std::shared_ptr<TThreadedServer> server(new TThreadedServer(proc_factory, sock, transport_factory, protocol_factory));
  return server;
}
}
}
