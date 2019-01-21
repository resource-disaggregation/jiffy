#ifndef JIFFY_DIRECTORY_LEASE_SERVICE_HANDLER_H
#define JIFFY_DIRECTORY_LEASE_SERVICE_HANDLER_H

#include "lease_service.h"
#include "../fs/directory_tree.h"
#include "../../storage/storage_management_ops.h"

namespace jiffy {
namespace directory {
/* Lease service handler class, inherited from lease_serviceIf class */
class lease_service_handler : public lease_serviceIf {
 public:

  /**
   * @brief Constructor
   * @param tree Directory tree
   * @param lease_period_ms Lease duration
   */

  explicit lease_service_handler(std::shared_ptr<directory_tree> tree, int64_t lease_period_ms);

  /**
   * @brief Renew leases
   * @param _return RPC lease acknowledgement to be collected
   * @param to_renew Paths to be renewed
   */

  void renew_leases(rpc_lease_ack &_return, const std::vector<std::string> &to_renew) override;

 private:
  /**
   * @brief Make exception
   */

  lease_service_exception make_exception(const directory_ops_exception &ex);
  /* Directory tree */
  std::shared_ptr<directory_tree> tree_;
  /* Lease duration */
  int64_t lease_period_ms_;
};

}
}

#endif //JIFFY_DIRECTORY_LEASE_SERVICE_HANDLER_H
