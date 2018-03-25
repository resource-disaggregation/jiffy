#ifndef ELASTICMEM_DIRECTORY_LEASE_SERVICE_FACTORY_H
#define ELASTICMEM_DIRECTORY_LEASE_SERVICE_FACTORY_H

#include "directory_lease_service.h"
#include "../fs/directory_tree.h"
#include "../../storage/storage_management_ops.h"

namespace elasticmem {
namespace directory {

class directory_lease_service_factory : public directory_lease_serviceIfFactory {
 public:
  explicit directory_lease_service_factory(std::shared_ptr<directory_tree> tree);
  directory_lease_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;
  void releaseHandler(directory_lease_serviceIf *anIf) override;

 private:
  std::shared_ptr<directory_tree> tree_;
};

}
}

#endif //ELASTICMEM_DIRECTORY_LEASE_SERVICE_FACTORY_H
