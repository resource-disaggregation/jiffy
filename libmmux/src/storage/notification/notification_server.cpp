#include "notification_server.h"
#include "notification_service.h"
#include "notification_service_factory.h"

#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>

namespace mmux {
namespace storage {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

std::shared_ptr<TThreadedServer> notification_server::create(std::vector<std::shared_ptr<chain_module>> &blocks,
                                                             const std::string &address,
                                                             int port) {
  std::shared_ptr<notification_serviceIfFactory> clone_factory(new notification_service_factory(blocks));
  std::shared_ptr<notification_serviceProcessorFactory>
      proc_factory(new notification_serviceProcessorFactory(clone_factory));
  std::shared_ptr<TServerSocket> sock(new TServerSocket(address, port));
  std::shared_ptr<TBufferedTransportFactory> transport_factory(new TBufferedTransportFactory());
  std::shared_ptr<TBinaryProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  std::shared_ptr<TThreadedServer> server(new TThreadedServer(proc_factory, sock, transport_factory, protocol_factory));
  return server;
}

}
}