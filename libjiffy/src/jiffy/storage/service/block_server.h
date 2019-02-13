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
   * @return Block server
   */
  static std::shared_ptr<apache::thrift::server::TServer> create(std::vector<std::shared_ptr<block>> &blocks,
                                                                 const std::string &address,
                                                                 int port);
};

}
}

#endif //JIFFY_BLOCK_SERVER_H
