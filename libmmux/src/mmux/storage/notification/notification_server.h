#ifndef MMUX_BROKER_SERVER_H
#define MMUX_BROKER_SERVER_H

#include "subscription_map.h"
#include "../chain_module.h"

#include <thrift/server/TThreadedServer.h>

namespace mmux {
namespace storage {

class notification_server {
 public:
  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::vector<std::shared_ptr<chain_module>> &blocks,
                                                                         const std::string &address,
                                                                         int port);
};

}
}

#endif //MMUX_BROKER_SERVER_H
