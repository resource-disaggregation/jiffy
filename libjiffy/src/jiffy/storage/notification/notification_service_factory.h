#ifndef JIFFY_NOTIFICATION_SERVICE_FACTORY_H
#define JIFFY_NOTIFICATION_SERVICE_FACTORY_H

#include "notification_service.h"
#include "subscription_map.h"
#include "jiffy/storage/block.h"

namespace jiffy {
namespace storage {
/* Notification service factory class
 * Inherited from notification_serviceIfFactory */
class notification_service_factory: public notification_serviceIfFactory {
 public:

  /**
   * @brief Constructor
   * @param blocks Data block
   */

  explicit notification_service_factory(std::vector<std::shared_ptr<block>> &blocks);

  /**
   * @brief Fetch notification service handler
   * @param conn_info Connection information
   * @return Notification service handler
   */

  notification_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &conn_info) override;

  /**
   * @brief Release handler
   * @param anIf Notification handler
   */

  void releaseHandler(notification_serviceIf *anIf) override;

 private:
  /* Data blocks */
  std::vector<std::shared_ptr<block>> &blocks_;
};

}
}

#endif //JIFFY_NOTIFICATION_SERVICE_FACTORY_H
