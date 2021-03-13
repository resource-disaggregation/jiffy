#include <thrift/transport/TSocket.h>
#include "maestro_allocator_service_factory.h"
#include "maestro_allocator_service_handler.h"
#include "../../utils/logger.h"

namespace jiffy {
  namespace maestro {

    using namespace ::apache::thrift;
    using namespace ::apache::thrift::transport;
    using namespace utils;

    maestro_allocator_service_factory::maestro_allocator_service_factory(std::shared_ptr<allocator_service> allocator)
      : allocator_(std::move(allocator)){}

    maestro_allocator_serviceIf *maestro_allocator_service_factory::getHandler(const apache::thrift::TConnectionInfo &conn_info) {
      std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(conn_info.transport);
      LOG(trace) << "Incoming connection from " << sock->getSocketInfo();
      return new maestro_allocator_service_handler(allocator_);
    }

    void maestro_allocator_service_factory::releaseHandler(maestro_allocator_serviceIf *handler) {
      LOG(trace) << "Releasing connection...";
      delete handler;
    }
  }
}