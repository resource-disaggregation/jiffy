#ifndef MMUX_DIRECTORY_LEASE_SERVICE_HANDLER_H
#define MMUX_DIRECTORY_LEASE_SERVICE_HANDLER_H

#include "lease_service.h"
#include "../fs/directory_tree.h"
#include "../../storage/storage_management_ops.h"

namespace mmux {
namespace directory {

class lease_service_handler : public lease_serviceIf {
 public:
  explicit lease_service_handler(std::shared_ptr<directory_tree> tree, int64_t lease_period_ms);
  void renew_leases(rpc_lease_ack &_return, const std::vector<std::string> &to_renew) override;

 private:
  lease_service_exception make_exception(const directory_ops_exception &ex);

  std::shared_ptr<directory_tree> tree_;
  int64_t lease_period_ms_;
};

}
}

#endif //MMUX_DIRECTORY_LEASE_SERVICE_HANDLER_H
