#ifndef JIFFY_BLOCK_SERVER_H
#define JIFFY_BLOCK_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include "jiffy/storage/block.h"

namespace jiffy {
namespace storage {
/* Block server class */
class block_server {
 public:
  /**
   * @brief Create block server
   * @param blocks Data blocks
   * @param address Socket address
   * @param port Socket port
   * @param non_blocking Bool value, true if non blocking server
   * @param num_io_threads Number of I/O threads
   * @param num_proc_threads Number of processing threads
   * @return Block server
   */

  static std::shared_ptr<apache::thrift::server::TServer> create(std::vector<std::shared_ptr<block>> &blocks,
                                                                 const std::string &address,
                                                                 int port,
                                                                 bool non_blocking = false,
                                                                 int num_io_threads = 1,
                                                                 int num_proc_threads = std::thread::hardware_concurrency());
};

}
}

#endif //JIFFY_BLOCK_SERVER_H
