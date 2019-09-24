#ifndef JIFFY_BLOCK_SERVER_H
#define JIFFY_BLOCK_SERVER_H

#include <thrift/server/TThreadedServer.h>
#include <thrift/server/TServer.h>
#include "jiffy/storage/block.h"

namespace jiffy {
namespace storage {

/* Block server class */
class block_server {
 public:
  typedef std::shared_ptr<block> block_ptr;
  typedef std::shared_ptr<apache::thrift::server::TServer> server_ptr;

  /**
   * @brief Create block server
   * @param blocks Data blocks
   * @param address Socket address
   * @param port Socket port
   * @return Block server
   */
  static server_ptr create(std::vector<block_ptr> &blocks, int port, size_t num_threads = 1);
};

}
}

#endif //JIFFY_BLOCK_SERVER_H
