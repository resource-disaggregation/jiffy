#ifndef ELASTICMEM_KV_RPC_SERVER_H
#define ELASTICMEM_KV_RPC_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include "../block/kv_block.h"

namespace elasticmem {
namespace kv {

class kv_management_rpc_server {
 public:
  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::vector<std::shared_ptr<kv_block>>& blocks,
                                                                         const std::string &address,
                                                                         int port);

};

}
}

#endif //ELASTICMEM_KV_RPC_SERVER_H
