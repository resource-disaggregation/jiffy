#ifndef ELASTICMEM_DIRECTORY_LEASE_SERVER_H
#define ELASTICMEM_DIRECTORY_LEASE_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include "../../kv/kv_management_service.h"
#include "../tree/directory_tree.h"

namespace elasticmem {
namespace directory {

class directory_lease_server {
 public:
  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::shared_ptr<directory_tree> tree,
                                                                         std::shared_ptr<kv::kv_management_service> kv,
                                                                         const std::string &address,
                                                                         int port);
};

}
}

#endif //ELASTICMEM_DIRECTORY_LEASE_SERVER_H
