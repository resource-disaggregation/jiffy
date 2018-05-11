#ifndef MMUX_NOTIFICATION_SERVICE_HANDLER_H
#define MMUX_NOTIFICATION_SERVICE_HANDLER_H

#include "notification_service.h"
#include "subscription_service.h"
#include "subscription_map.h"
#include "../chain_module.h"
#include <thrift/transport/TSocket.h>

namespace mmux {
namespace storage {

class notification_service_handler : public notification_serviceIf {
 public:
  explicit notification_service_handler(std::shared_ptr<::apache::thrift::protocol::TProtocol> oprot,
                                        std::vector<std::shared_ptr<chain_module>> &blocks);
  void subscribe(int32_t block_id, const std::vector<std::string> &ops) override;
  void unsubscribe(int32_t block_id, const std::vector<std::string> &ops) override;

 private:
  std::set<std::pair<int32_t, std::string>> local_subs_;
  std::shared_ptr<::apache::thrift::protocol::TProtocol> oprot_;
  std::shared_ptr<subscription_serviceClient> client_;
  std::vector<std::shared_ptr<chain_module>> &blocks_;
};

}
}

#endif //MMUX_NOTIFICATION_SERVICE_HANDLER_H
