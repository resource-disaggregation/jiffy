#ifndef JIFFY_AUTO_SCALING_RPC_SERVER_H
#define JIFFY_AUTO_SCALING_RPC_SERVER_H

#include <thrift/server/TThreadedServer.h>

namespace jiffy {
namespace auto_scaling {
/* Storage management server class */
class auto_scaling_server {
 public:
  /**
   * @brief Create a storage management server
   * @param blocks Blocks
   * @param address Socket address
   * @param port Socket port number
   * @return Server
   */
  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(const std::string directory_host,
                                                                         int directory_port,
                                                                         const std::string &address,
                                                                         int port);

};

}
}

#endif //JIFFY_AUTO_SCALING_RPC_SERVER_H
