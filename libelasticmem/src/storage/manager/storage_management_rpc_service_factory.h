#ifndef ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_FACTORY_H
#define ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_FACTORY_H

#include "../block_management_ops.h"
#include "storage_management_rpc_service.h"

namespace elasticmem {
namespace storage {

class storage_management_rpc_service_factory : public storage_management_rpc_serviceIfFactory {
 public:
  explicit storage_management_rpc_service_factory(std::vector<std::shared_ptr<block_management_ops>> &blocks);
  storage_management_rpc_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;
  void releaseHandler(storage_management_rpc_serviceIf *anIf) override;
 private:
  std::vector<std::shared_ptr<block_management_ops>> &blocks_;
};

}
}

#endif //ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_FACTORY_H
