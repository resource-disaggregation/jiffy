#ifndef MMUX_DIRECTORY_LEASE_SERVER_H
#define MMUX_DIRECTORY_LEASE_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include "../../storage/storage_management_ops.h"
#include "../fs/directory_tree.h"

namespace mmux {
namespace directory {

class lease_server {
 public:
  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::shared_ptr<directory_tree> tree,
                                                                         int64_t lease_period_ms,
                                                                         const std::string &address,
                                                                         int port);
};

}
}

#endif //MMUX_DIRECTORY_LEASE_SERVER_H
