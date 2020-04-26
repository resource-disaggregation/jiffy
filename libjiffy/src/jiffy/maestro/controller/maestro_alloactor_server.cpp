#include "maestro_alloactor_server.h"
#include "maestro_allocator_service_factory.h"
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>

namespace jiffy {
  namespace maestro {

    using namespace ::apache::thrift;
    using namespace ::apache::thrift::protocol;
    using namespace ::apache::thrift::transport;
    using namespace ::apache::thrift::server;

    std::shared_ptr<apache::thrift::server::TThreadedServer> maestro_alloactor_server::create(std::shared_ptr<allocator_service> allocator,
                                                                                              const std::string &address,
                                                                                              int port) {
      std::shared_ptr<maestro_allocator_serviceIfFactory>
          clone_factory(new maestro_allocator_service_factory(std::move(allocator)));
      std::shared_ptr<maestro_allocator_serviceProcessorFactory>
          proc_factory(new maestro_allocator_serviceProcessorFactory(clone_factory));
      std::shared_ptr<TServerSocket> sock(new TServerSocket(address, port));
      std::shared_ptr<TBufferedTransportFactory> transport_factory(new TBufferedTransportFactory());
      std::shared_ptr<TBinaryProtocolFactory> protocol_factory(new TBinaryProtocolFactory());
      std::shared_ptr<TThreadedServer> server(new TThreadedServer(proc_factory, sock, transport_factory, protocol_factory));
      return server;
    }
  }
}