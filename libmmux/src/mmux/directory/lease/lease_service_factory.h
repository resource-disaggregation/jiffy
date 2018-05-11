#ifndef MMUX_DIRECTORY_LEASE_SERVICE_FACTORY_H
#define MMUX_DIRECTORY_LEASE_SERVICE_FACTORY_H

#include "lease_service.h"
#include "../fs/directory_tree.h"
#include "../../storage/storage_management_ops.h"

namespace mmux {
namespace directory {

class lease_service_factory : public lease_serviceIfFactory {
 public:
  explicit lease_service_factory(std::shared_ptr<directory_tree> tree, int64_t lease_period_ms);
  lease_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;
  void releaseHandler(lease_serviceIf *anIf) override;

 private:
  std::shared_ptr<directory_tree> tree_;
  int64_t lease_period_ms_;
};

}
}

#endif //MMUX_DIRECTORY_LEASE_SERVICE_FACTORY_H
