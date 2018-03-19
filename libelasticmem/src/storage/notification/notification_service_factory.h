#ifndef ELASTICMEM_NOTIFICATION_SERVICE_FACTORY_H
#define ELASTICMEM_NOTIFICATION_SERVICE_FACTORY_H

#include "notification_service.h"
#include "subscription_map.h"

namespace elasticmem {
namespace storage {

class notification_service_factory: public notification_serviceIfFactory {
 public:
  explicit notification_service_factory(std::vector<std::shared_ptr<subscription_map>> &subs);
  notification_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &conn_info) override;
  void releaseHandler(notification_serviceIf *anIf) override;

 private:
  std::vector<std::shared_ptr<subscription_map>> &subs_;
};

}
}

#endif //ELASTICMEM_NOTIFICATION_SERVICE_FACTORY_H
