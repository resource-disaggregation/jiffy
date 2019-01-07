#ifndef MMUX_CHAIN_SERVER_H
#define MMUX_CHAIN_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include "../chain_module.h"

namespace mmux {
namespace storage {
/* Chain server class */
class chain_server {
 public:

  /**
   * @brief Create chain server
   * @param blocks Data blocks
   * @param address Address
   * @param port Port number
   * @param non_blocking Bool value, true if server is non_blocking
   * @param num_io_threads Number of I/O threads
   * @param num_proc_threads Number of processing threads
   * @return Chain server
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

#endif //MMUX_CHAIN_SERVER_H