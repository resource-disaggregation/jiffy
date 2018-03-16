#include "directory_lease_server.h"
#include "directory_lease_service_factory.h"

#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>

namespace elasticmem {
namespace directory {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

std::shared_ptr<TThreadedServer> directory_lease_server::create(std::shared_ptr<directory_tree> tree,
                                                                const std::string &address, int port) {
  std::shared_ptr<directory_lease_serviceIfFactory>
      clone_factory(new directory_lease_service_factory(std::move(tree)));
  std::shared_ptr<directory_lease_serviceProcessorFactory>
      proc_factory(new directory_lease_serviceProcessorFactory(clone_factory));
  std::shared_ptr<TServerSocket> sock(new TServerSocket(address, port));
  std::shared_ptr<TBufferedTransportFactory> transport_factory(new TBufferedTransportFactory());
  std::shared_ptr<TBinaryProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  std::shared_ptr<TThreadedServer> server(new TThreadedServer(proc_factory, sock, transport_factory, protocol_factory));
  return server;
}
}
}