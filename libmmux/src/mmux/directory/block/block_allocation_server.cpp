#include "block_allocation_server.h"
#include "block_allocation_service_factory.h"

#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>

namespace mmux {
namespace directory {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

std::shared_ptr<apache::thrift::server::TThreadedServer> block_allocation_server::create(std::shared_ptr<block_allocator> alloc,
                                                                                         const std::string &address,
                                                                                         int port) {
  std::shared_ptr<block_allocation_serviceIfFactory>
      clone_factory(new block_allocation_service_factory(std::move(alloc)));
  std::shared_ptr<block_allocation_serviceProcessorFactory>
      proc_factory(new block_allocation_serviceProcessorFactory(clone_factory));
  std::shared_ptr<TServerSocket> sock(new TServerSocket(address, port));
  std::shared_ptr<TBufferedTransportFactory> transport_factory(new TBufferedTransportFactory());
  std::shared_ptr<TBinaryProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  std::shared_ptr<TThreadedServer> server(new TThreadedServer(proc_factory, sock, transport_factory, protocol_factory));
  return server;
}
}
}