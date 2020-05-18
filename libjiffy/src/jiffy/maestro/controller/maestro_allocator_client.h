#ifndef JIFFY_MAESTRO_ALLOCATOR_CLIENT_H
#define JIFFY_MAESTRO_ALLOCATOR_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "maestro_allocator_service.h"

namespace jiffy {
  namespace maestro {
    class maestro_allocator_client {
    public:
      typedef maestro_allocator_serviceClient thrift_client;

      maestro_allocator_client() = default;

      ~maestro_allocator_client();

      /**
       * @brief Constructor
       * @param hostname Maestro allocation server hostname
       * @param port port number
       */

      maestro_allocator_client(const std::string &hostname, int port);

      /**
       * @brief Connect Block allocation server
       * @param hostname Block allocation server hostname
       * @param port Port number
       */

      void connect(const std::string &hostname, int port);

      /**
       * @brief Disconnect server
       */

      void disconnect();

      std::vector<std::string> allocate(const std::string &tenantID, const int64_t numBlocks);

      void deallocate(const std::string &tenentID, const std::vector<std::string> &blocks);


    private:
      /* Socket */
      std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
      /* Transport */
      std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
      /* Protocol */
      std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
      /* Client */
      std::shared_ptr<thrift_client> client_{};

    };
  }
}


#endif //JIFFY_MAESTRO_ALLOCATOR_CLIENT_H
