#ifndef JIFFY_DIRECTORY_LEASE_SERVICE_FACTORY_H
#define JIFFY_DIRECTORY_LEASE_SERVICE_FACTORY_H

#include "lease_service.h"
#include "../fs/directory_tree.h"
#include "../../storage/storage_management_ops.h"

namespace jiffy {
namespace directory {

/* Lease service factory class, inherited from lease_serviceIfFactory class */

class lease_service_factory : public lease_serviceIfFactory {
 public:

  /**
   * @brief Constructor
   * @param tree Directory tree
   * @param lease_period_ms Lease duration
   */

  explicit lease_service_factory(std::shared_ptr<directory_tree> tree, int64_t lease_period_ms);

  /**
   * @brief Get lease service handler
   * @param conn_info Connection information
   * @return Lease service handler
   */

  lease_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;

  /**
   * @brief Release handler
   * @param handler Handler
   */

  void releaseHandler(lease_serviceIf *anIf) override;

 private:
  /* Directory tree */
  std::shared_ptr<directory_tree> tree_;
  /* Lease duration */
  int64_t lease_period_ms_;
};

}
}

#endif //JIFFY_DIRECTORY_LEASE_SERVICE_FACTORY_H
