#include "kv_rpc_server.h"
#include "kv_rpc_service.h"
#include "kv_rpc_service_factory.h"

#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>

namespace elasticmem {
namespace kv {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

std::shared_ptr<TThreadedServer> kv_rpc_server::create(std::vector<std::shared_ptr<kv_block>> &blocks,
                                                       const std::string &address,
                                                       int port) {
  std::shared_ptr<kv_rpc_serviceIfFactory>
      clone_factory(new kv_rpc_service_factory(blocks));
  std::shared_ptr<kv_rpc_serviceProcessorFactory>
      proc_factory(new kv_rpc_serviceProcessorFactory(clone_factory));
  std::shared_ptr<TServerSocket> sock(new TServerSocket(address, port));
  std::shared_ptr<TBufferedTransportFactory> transport_factory(new TBufferedTransportFactory());
  std::shared_ptr<TBinaryProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  std::shared_ptr<TThreadedServer> server(new TThreadedServer(proc_factory, sock, transport_factory, protocol_factory));
  return server;
}

}
}
