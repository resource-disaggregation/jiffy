#ifndef JIFFY_BROKER_SERVER_H
#define JIFFY_BROKER_SERVER_H

#include "subscription_map.h"
#include "jiffy/storage/block.h"

#include <thrift/server/TThreadedServer.h>

namespace jiffy {
namespace storage {
/* Notification server class */
class notification_server {
 public:

  /**
   * @brief Create notification server
   * @param blocks Chain modules
   * @param address Socket address
   * @param port Socket Port number
   * @return Server
   */

  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::vector<std::shared_ptr<block>> &blocks,
                                                                         const std::string &address,
                                                                         int port);
};

}
}

#endif //JIFFY_BROKER_SERVER_H
