#include "block_server.h"
#include "block_service.h"
#include "block_service_factory.h"

#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>

namespace elasticmem {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

std::shared_ptr<TThreadedServer> block_server::create(std::vector<std::shared_ptr<kv_block>> &blocks,
                                                       std::vector<std::shared_ptr<subscription_map>> &sub_maps,
                                                       const std::string &address,
                                                       int port) {
  std::shared_ptr<block_serviceIfFactory> clone_factory(new block_service_factory(blocks, sub_maps));
  std::shared_ptr<block_serviceProcessorFactory> proc_factory(new block_serviceProcessorFactory(clone_factory));
  std::shared_ptr<TServerSocket> sock(new TServerSocket(address, port));
  std::shared_ptr<TBufferedTransportFactory> transport_factory(new TBufferedTransportFactory());
  std::shared_ptr<TBinaryProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  std::shared_ptr<TThreadedServer> server(new TThreadedServer(proc_factory, sock, transport_factory, protocol_factory));
  return server;
}

}
}
