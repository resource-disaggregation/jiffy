#ifndef ELASTICMEM_KV_MANAGEMENT_RPC_SERVER_H
#define ELASTICMEM_KV_MANAGEMENT_RPC_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include "../block/block.h"

namespace elasticmem {
namespace storage {

class storage_management_rpc_server {
 public:
  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::vector<std::shared_ptr<block>> &blocks,
                                                                         const std::string &address,
                                                                         int port);

};

}
}

#endif //ELASTICMEM_KV_MANAGEMENT_RPC_SERVER_H
