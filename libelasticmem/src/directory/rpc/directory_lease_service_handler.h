#ifndef ELASTICMEM_DIRECTORY_LEASE_SERVICE_HANDLER_H
#define ELASTICMEM_DIRECTORY_LEASE_SERVICE_HANDLER_H

#include "directory_lease_service.h"
#include "../tree/directory_tree.h"
#include "../../kv/kv_management_service.h"

namespace elasticmem {
namespace directory {

class directory_lease_service_handler : public directory_lease_serviceIf {
 public:
  explicit directory_lease_service_handler(std::shared_ptr<directory_tree> tree,
                                           std::shared_ptr<kv::kv_management_service> kv);
  void update_leases(rpc_lease_ack &_return, const rpc_lease_update &updates) override;

 private:
  directory_lease_service_exception make_exception(const directory_service_exception &ex);

  std::shared_ptr<directory_tree> tree_;
  std::shared_ptr<kv::kv_management_service> kv_;
};

}
}

#endif //ELASTICMEM_DIRECTORY_LEASE_SERVICE_HANDLER_H
