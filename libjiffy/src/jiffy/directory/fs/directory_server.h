#ifndef JIFFY_DIRECTORY_RPC_SERVER_H
#define JIFFY_DIRECTORY_RPC_SERVER_H

#include <string>
#include <thrift/server/TThreadedServer.h>
#include "directory_tree.h"

namespace jiffy {
namespace directory {

/* Directory server class */

class directory_server {
 public:

  /**
   * @brief Create a directory server
   * @param shard Directory tree
   * @param address socket address
   * @param port socket port number
   * @return Directory server
   */

  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::shared_ptr<directory_tree> shard,
                                                                         const std::string &address,
                                                                         int port);
};

}
}

#endif //JIFFY_DIRECTORY_RPC_SERVER_H
