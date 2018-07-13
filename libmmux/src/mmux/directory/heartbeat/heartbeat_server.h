#ifndef MEMORYMUX_HEARTBEAT_SERVER_H
#define MEMORYMUX_HEARTBEAT_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include "../block/block_allocator.h"
#include "../directory_ops.h"
#include "health_metadata.h"

namespace mmux {
namespace directory {

class heartbeat_server {
 public:
  typedef std::map<std::string, health_metadata> hm_map_t;

  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::shared_ptr<block_allocator> alloc,
                                                                         std::shared_ptr<directory_management_ops> mgmt,
                                                                         std::shared_ptr<hm_map_t> hm_map,
                                                                         const std::string &address,
                                                                         int port);
};

}
}

#endif //MEMORYMUX_HEARTBEAT_SERVER_H
