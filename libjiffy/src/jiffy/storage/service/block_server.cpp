#include "block_server.h"
#include "block_request_handler_factory.h"
#include "jiffy/utils/logger.h"

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/server/TThreadPoolServer.h>

#include "buffered_transport_factory.h"

namespace jiffy {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;
using namespace jiffy::utils;

std::shared_ptr<TServer> block_server::create(std::vector<std::shared_ptr<block>> &blocks,
                                              const std::string &address,
                                              int port) {
  LOG(log_level::info) << "Creating threaded server";
  auto clone_factory = std::make_shared<block_request_handler_factory>(blocks);
  auto proc_factory = std::make_shared<block_request_serviceProcessorFactory>(clone_factory);
  auto protocol_factory = std::make_shared<TBinaryProtocolFactory>();
  auto sock = std::make_shared<TServerSocket>(address, port);
  auto transport_factory = std::make_shared<BufferedTransportFactory>(1024 * 1024);
  return std::make_shared<TThreadedServer>(proc_factory, sock, transport_factory, protocol_factory);
}

}
}
