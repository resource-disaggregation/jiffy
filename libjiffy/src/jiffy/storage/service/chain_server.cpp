#include "chain_server.h"
#include "chain_request_handler_factory.h"
#include "buffered_transport_factory.h"

#include <thrift/transport/TBufferTransports.h>
#include <thrift/server/TThreadPoolServer.h>

namespace jiffy {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

std::shared_ptr<TServer> chain_server::create(std::vector<std::shared_ptr<block>> &blocks,
                                              const std::string &address,
                                              int port) {
  auto clone_factory = std::make_shared<chain_request_handler_factory>(blocks);
  auto proc_factory = std::make_shared<chain_request_serviceProcessorFactory>(clone_factory);
  auto protocol_factory = std::make_shared<TBinaryProtocolFactory>();
  auto sock = std::make_shared<TServerSocket>(address, port);
  auto transport_factory = std::make_shared<BufferedTransportFactory>(1024 * 1024);
  return std::make_shared<TThreadedServer>(proc_factory, sock, transport_factory, protocol_factory);
}
}
}
