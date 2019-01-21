#ifndef JIFFY_DIRECTORY_LEASE_SERVER_H
#define JIFFY_DIRECTORY_LEASE_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include "../../storage/storage_management_ops.h"
#include "../fs/directory_tree.h"

namespace jiffy {
namespace directory {
/* Lease server class */
class lease_server {
 public:

  /**
   * @brief Create lease server
   * @param tree Directory tree
   * @param lease_period_ms Lease duration
   * @param address Socket address
   * @param port Socket port number
   * @return Server socket
   */

  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::shared_ptr<directory_tree> tree,
                                                                         int64_t lease_period_ms,
                                                                         const std::string &address,
                                                                         int port);
};

}
}

#endif //JIFFY_DIRECTORY_LEASE_SERVER_H
