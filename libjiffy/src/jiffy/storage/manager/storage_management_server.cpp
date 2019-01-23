#include "jiffy/storage/manager/storage_management_server.h"
#include "jiffy/storage/manager/storage_management_service.h"
#include "jiffy/storage/manager/storage_management_service_factory.h"

#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>

namespace jiffy {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

std::shared_ptr<TThreadedServer> storage_management_server::create(std::vector<std::shared_ptr<block>> &blocks,
                                                                   const std::string &address,
                                                                   int port) {
  std::shared_ptr<storage_management_serviceIfFactory> clone_factory(new storage_management_service_factory(blocks));
  std::shared_ptr<storage_management_serviceProcessorFactory>
      proc_factory(new storage_management_serviceProcessorFactory(clone_factory));
  std::shared_ptr<TServerSocket> sock(new TServerSocket(address, port));
  std::shared_ptr<TBufferedTransportFactory> transport_factory(new TBufferedTransportFactory());
  std::shared_ptr<TBinaryProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  std::shared_ptr<TThreadedServer> server(new TThreadedServer(proc_factory, sock, transport_factory, protocol_factory));
  return server;
}

}
}
