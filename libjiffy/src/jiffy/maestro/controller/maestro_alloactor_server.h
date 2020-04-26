#ifndef JIFFY_MAESTRO_ALLOACTOR_SERVER_H
#define JIFFY_MAESTRO_ALLOACTOR_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include <jiffy/maestro/allocator_service.h>

namespace jiffy {
  namespace maestro {

    class maestro_alloactor_server {
    public:
      /**
       * @brief Create block allocation server
       * @param alloc Block allocator
       * @param address Socket address
       * @param port Socket port number
       * @return Block allocation server
       */
      static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::shared_ptr<allocator_service> allocator,
                                                                             const std::string &address,
                                                                             int port);

    };

  }
}


#endif //JIFFY_MAESTRO_ALLOACTOR_SERVER_H
