#include "heartbeat_server.h"
#include "heartbeat_service.h"
#include "heartbeat_service_factory.h"

#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>

namespace mmux {
namespace directory {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

std::shared_ptr<TThreadedServer> heartbeat_server::create(std::shared_ptr<block_allocator> alloc,
                                                          std::shared_ptr<directory_management_ops> mgmt,
                                                          std::shared_ptr<hm_map_t> hm_map,
                                                          const std::string &address,
                                                          int port) {
  std::shared_ptr<heartbeat_serviceIfFactory> clone_factory(new heartbeat_service_factory(alloc, mgmt, hm_map));
  std::shared_ptr<heartbeat_serviceProcessorFactory> proc_factory(new heartbeat_serviceProcessorFactory(clone_factory));
  std::shared_ptr<TServerSocket> sock(new TServerSocket(address, port));
  std::shared_ptr<TBufferedTransportFactory> transport_factory(new TBufferedTransportFactory());
  std::shared_ptr<TBinaryProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  std::shared_ptr<TThreadedServer> server(new TThreadedServer(proc_factory, sock, transport_factory, protocol_factory));
  return server;
}
}
}
