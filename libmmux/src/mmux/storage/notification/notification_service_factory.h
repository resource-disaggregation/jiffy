#ifndef MMUX_NOTIFICATION_SERVICE_FACTORY_H
#define MMUX_NOTIFICATION_SERVICE_FACTORY_H

#include "notification_service.h"
#include "subscription_map.h"
#include "../chain_module.h"

namespace mmux {
namespace storage {
/* */
class notification_service_factory: public notification_serviceIfFactory {
 public:

  /**
   * @brief
   * @param blocks
   */

  explicit notification_service_factory(std::vector<std::shared_ptr<chain_module>> &blocks);

  /**
   * @brief
   * @param conn_info
   * @return
   */

  notification_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &conn_info) override;

  /**
   * @brief
   * @param anIf
   */

  void releaseHandler(notification_serviceIf *anIf) override;

 private:
  /* */
  std::vector<std::shared_ptr<chain_module>> &blocks_;
};

}
}

#endif //MMUX_NOTIFICATION_SERVICE_FACTORY_H
