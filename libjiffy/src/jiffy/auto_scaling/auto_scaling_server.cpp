#include "jiffy/auto_scaling/auto_scaling_server.h"
#include "jiffy/auto_scaling/auto_scaling_service.h"
#include "jiffy/auto_scaling/auto_scaling_service_factory.h"

#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>

namespace jiffy {
namespace auto_scaling {

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

std::shared_ptr<TThreadedServer> auto_scaling_server::create(const std::string& directory_host,
                                                             int directory_port,
                                                             const std::string &address,
                                                             int port) {
  std::shared_ptr<auto_scaling_serviceIfFactory>
      clone_factory(new auto_scaling_service_factory(directory_host, directory_port));
  std::shared_ptr<auto_scaling_serviceProcessorFactory>
      proc_factory(new auto_scaling_serviceProcessorFactory(clone_factory));
  std::shared_ptr<TServerSocket> sock(new TServerSocket(address, port));
  std::shared_ptr<TBufferedTransportFactory> transport_factory(new TBufferedTransportFactory());
  std::shared_ptr<TBinaryProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
  std::shared_ptr<TThreadedServer> server(new TThreadedServer(proc_factory, sock, transport_factory, protocol_factory));
  return server;
}

}
}
