#ifndef JIFFY_AUTO_SCALING_RPC_SERVER_H
#define JIFFY_AUTO_SCALING_RPC_SERVER_H

#include <thrift/server/TThreadedServer.h>

namespace jiffy {
namespace auto_scaling {
/* Auto scaling server class */
class auto_scaling_server {
 public:
  /**
   * @brief Create an auto scaling server
   * @param directory_host Directory server host address
   * @param directory_port Directory server port number
   * @param address Auto scaling server host address
   * @param port Auto scaling server port number
   * @return Server
   */
  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(const std::string& directory_host,
                                                                         int directory_port,
                                                                         const std::string &address,
                                                                         int port);

};

}
}

#endif //JIFFY_AUTO_SCALING_RPC_SERVER_H
