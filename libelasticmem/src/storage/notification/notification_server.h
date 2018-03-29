#ifndef ELASTICMEM_BROKER_SERVER_H
#define ELASTICMEM_BROKER_SERVER_H

#include "subscription_map.h"
#include "../chain_module.h"

#include <thrift/server/TThreadedServer.h>

namespace elasticmem {
namespace storage {

class notification_server {
 public:
  static std::shared_ptr<apache::thrift::server::TThreadedServer> create(std::vector<std::shared_ptr<chain_module>> &blocks,
                                                                         const std::string &address,
                                                                         int port);
};

}
}

#endif //ELASTICMEM_BROKER_SERVER_H
