#ifndef ELASTICMEM_DIRECTORY_RPC_SERVER_H
#define ELASTICMEM_DIRECTORY_RPC_SERVER_H

#include <string>
#include <thrift/server/TThreadedServer.h>
#include "../tree/directory_tree.h"

namespace elasticmem {
namespace directory {

class directory_rpc_server {
 public:
  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::shared_ptr<directory_tree> shard,
                                                                         const std::string &address,
                                                                         int port);
};

}
}

#endif //ELASTICMEM_DIRECTORY_RPC_SERVER_H
