#include "chain_server.h"
#include "chain_request_handler_factory.h"
#include "buffered_transport_factory.h"

#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TNonblockingServerSocket.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/server/TThreadPoolServer.h>

namespace mmux {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

std::shared_ptr<TServer> chain_server::create(std::vector<std::shared_ptr<chain_module>> &blocks,
                                              const std::string &address,
                                              int port,
                                              bool non_blocking,
                                              int num_io_threads,
                                              int num_proc_threads) {
  std::shared_ptr<chain_request_serviceIfFactory> clone_factory(new chain_request_handler_factory(blocks));
  std::shared_ptr<chain_request_serviceProcessorFactory>
      proc_factory(new chain_request_serviceProcessorFactory(clone_factory));
  std::shared_ptr<TBinaryProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  if (non_blocking) {
    auto thread_manager = ThreadManager::newSimpleThreadManager(static_cast<size_t>(num_proc_threads));
    auto thread_factory = std::make_shared<PosixThreadFactory>();
    thread_manager->threadFactory(thread_factory);
    thread_manager->start();
    std::shared_ptr<TNonblockingServerSocket> socket(new TNonblockingServerSocket(port));
    std::shared_ptr<TNonblockingServer> server(new TNonblockingServer(proc_factory, protocol_factory, socket, thread_manager));
    server->setNumIOThreads(static_cast<size_t>(num_io_threads));
    server->setUseHighPriorityIOThreads(true);
    return server;
  } else {
    std::shared_ptr<TServerSocket> sock(new TServerSocket(address, port));
    std::shared_ptr<BufferedTransportFactory> transport_factory(new BufferedTransportFactory(1024 * 1024));
    std::shared_ptr<TServer> server(new TThreadedServer(proc_factory, sock, transport_factory, protocol_factory));
    return server;
  }
}
}
}