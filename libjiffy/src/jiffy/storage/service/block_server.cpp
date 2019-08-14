#include "block_server.h"
#include "block_request_handler_factory.h"
#include "jiffy/utils/logger.h"

#include <thrift/concurrency/ThreadManager.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/transport/TNonblockingServerSocket.h>

namespace jiffy {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;
using namespace utils;

std::shared_ptr<TServer> block_server::create(std::vector<std::shared_ptr<block>> &blocks, int port, size_t num_threads) {
  LOG(log_level::info) << "Creating non-blocking server";
  auto clone_factory = std::make_shared<block_request_handler_factory>(blocks);
  auto proc_factory = std::make_shared<block_request_serviceProcessorFactory>(clone_factory);
  auto socket = std::make_shared<TNonblockingServerSocket>(port);
  auto server = std::make_shared<TNonblockingServer>(proc_factory, socket);
  server->setUseHighPriorityIOThreads(true);
  server->setNumIOThreads(num_threads);
  return server;
}

}
}
