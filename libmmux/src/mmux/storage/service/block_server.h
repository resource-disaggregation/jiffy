#ifndef MMUX_BLOCK_SERVER_H
#define MMUX_BLOCK_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include "../chain_module.h"

namespace mmux {
namespace storage {

class block_server {
 public:
  static std::shared_ptr<apache::thrift::server::TServer> create(std::vector<std::shared_ptr<chain_module>> &blocks,
                                                                 const std::string &address,
                                                                 int port,
                                                                 bool non_blocking = false,
                                                                 int num_io_threads = 1,
                                                                 int num_proc_threads = std::thread::hardware_concurrency());
};

}
}

#endif //MMUX_BLOCK_SERVER_H
