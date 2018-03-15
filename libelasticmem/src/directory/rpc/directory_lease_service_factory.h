#ifndef ELASTICMEM_DIRECTORY_LEASE_SERVICE_FACTORY_H
#define ELASTICMEM_DIRECTORY_LEASE_SERVICE_FACTORY_H

#include "directory_lease_service.h"
#include "../tree/directory_tree.h"
#include "../../kv/kv_management_service.h"

namespace elasticmem {
namespace directory {

class directory_lease_service_factory : public directory_lease_serviceIfFactory {
 public:
  directory_lease_service_factory(std::shared_ptr<directory_tree> tree, std::shared_ptr<kv::kv_management_service> kv);
  directory_lease_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;
  void releaseHandler(directory_lease_serviceIf *anIf) override;

 private:
  std::shared_ptr<directory_tree> tree_;
  std::shared_ptr<kv::kv_management_service> kv_;
};

}
}

#endif //ELASTICMEM_DIRECTORY_LEASE_SERVICE_FACTORY_H
