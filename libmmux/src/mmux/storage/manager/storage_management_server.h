#ifndef MMUX_KV_MANAGEMENT_RPC_SERVER_H
#define MMUX_KV_MANAGEMENT_RPC_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include "../block.h"
#include "../chain_module.h"

namespace mmux {
namespace storage {
/* Storage management server class */
class storage_management_server {
 public:
  /**
   * @brief
   * @param blocks
   * @param address
   * @param port
   * @return
   */
  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::vector<std::shared_ptr<chain_module>> &blocks,
                                                                         const std::string &address,
                                                                         int port);

};

}
}

#endif //MMUX_KV_MANAGEMENT_RPC_SERVER_H
