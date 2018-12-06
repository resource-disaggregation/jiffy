#ifndef MMUX_BLOCK_SERVER_H
#define MMUX_BLOCK_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include "../chain_module.h"

namespace mmux {
namespace storage {
/* */
class block_server {
 public:
  /**
   * @brief Create block server
   * @param blocks Data blocks -> used to create block request handler factory
   * @param address Socket address
   * @param port Socket port
   * @param non_blocking TODO
   * @param num_io_threads Number of IO threads TODO
   * @param num_proc_threads Number of process threads
   * @return Block server
   */

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
