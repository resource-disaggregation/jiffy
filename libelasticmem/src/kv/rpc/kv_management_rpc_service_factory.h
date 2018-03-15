#ifndef ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_FACTORY_H
#define ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_FACTORY_H

#include "../block/kv_block.h"
#include "kv_management_rpc_service.h"

namespace elasticmem {
namespace kv {

class kv_management_rpc_service_factory: public kv_management_rpc_serviceIfFactory {
 public:
  explicit kv_management_rpc_service_factory(std::vector<std::shared_ptr<kv_block>>& blocks);
  kv_management_rpc_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;
  void releaseHandler(kv_management_rpc_serviceIf *anIf) override;
 private:
  std::vector<std::shared_ptr<kv_block>>& blocks_;
};

}
}

#endif //ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_FACTORY_H
