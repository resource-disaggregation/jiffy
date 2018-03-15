#ifndef ELASTICMEM_BLOCK_ALLOCATION_SERVER_H
#define ELASTICMEM_BLOCK_ALLOCATION_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include "block_allocator.h"

namespace elasticmem {
namespace directory {

class block_allocation_server {
 public:
  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::shared_ptr<block_allocator> alloc,
                                                                         const std::string &address,
                                                                         int port);

};

}
}

#endif //ELASTICMEM_BLOCK_ALLOCATION_SERVER_H
