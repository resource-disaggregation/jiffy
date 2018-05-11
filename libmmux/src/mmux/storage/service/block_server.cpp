#include "block_server.h"
#include "block_request_handler_factory.h"
#include "../../utils/logger.h"

#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/transport/TNonblockingServerSocket.h>
#include <memory>

#include "buffered_transport_factory.h"
namespace mmux {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;
using namespace mmux::utils;

std::shared_ptr<TServer> block_server::create(std::vector<std::shared_ptr<chain_module>> &blocks,
                                              const std::string &address,
                                              int port,
                                              bool non_blocking,
                                              int num_io_threads,
                                              int num_proc_threads) {
  std::shared_ptr<block_request_serviceIfFactory> clone_factory(new block_request_handler_factory(blocks));
  std::shared_ptr<block_request_serviceProcessorFactory>
      proc_factory(new block_request_serviceProcessorFactory(clone_factory));
  std::shared_ptr<TBinaryProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  if (non_blocking) {
    LOG(log_level::info) << "Creating non blocking server with " << num_io_threads << " io threads and "
                         << num_proc_threads << " processing threads";
    auto thread_manager = ThreadManager::newSimpleThreadManager(static_cast<size_t>(num_proc_threads));
    auto thread_factory = std::make_shared<PosixThreadFactory>();
    thread_manager->threadFactory(thread_factory);
    thread_manager->start();
    std::shared_ptr<TNonblockingServerSocket> socket(new TNonblockingServerSocket(port));
    std::shared_ptr<TNonblockingServer>
        server(new TNonblockingServer(proc_factory, protocol_factory, socket, thread_manager));
    server->setNumIOThreads(static_cast<size_t>(num_io_threads));
    server->setUseHighPriorityIOThreads(true);
    return server;
  } else {
    LOG(log_level::info) << "Creating threaded server";
    std::shared_ptr<TServerSocket> sock(new TServerSocket(address, port));
    std::shared_ptr<BufferedTransportFactory> transport_factory(new BufferedTransportFactory(1024 * 1024));
    std::shared_ptr<TServer>
        server(new TThreadedServer(proc_factory, sock, transport_factory, protocol_factory));
    return server;
  }
}

}
}