#ifndef ELASTICMEM_KV_RPC_SERVER_H
#define ELASTICMEM_KV_RPC_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include "../chain_module.h"
#include "../notification/subscription_map.h"

namespace elasticmem {
namespace storage {

class block_server {
 public:
  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::vector<std::shared_ptr<chain_module>> &blocks,
                                                                         std::vector<std::shared_ptr<subscription_map>> &sub_maps,
                                                                         const std::string &address,
                                                                         int port);

};

}
}

#endif //ELASTICMEM_KV_RPC_SERVER_H
