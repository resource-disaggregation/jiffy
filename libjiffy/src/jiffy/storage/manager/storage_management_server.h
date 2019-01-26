#ifndef JIFFY_KV_MANAGEMENT_RPC_SERVER_H
#define JIFFY_KV_MANAGEMENT_RPC_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include "jiffy/storage/block.h"

namespace jiffy {
namespace storage {
/* Storage management server class */
class storage_management_server {
 public:
  /**
   * @brief Create a storage management server
   * @param blocks Blocks
   * @param address Socket address
   * @param port Socket port number
   * @return Server
   */
  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::vector<std::shared_ptr<block>> &blocks,
                                                                         const std::string &address,
                                                                         int port);

};

}
}

#endif //JIFFY_KV_MANAGEMENT_RPC_SERVER_H
