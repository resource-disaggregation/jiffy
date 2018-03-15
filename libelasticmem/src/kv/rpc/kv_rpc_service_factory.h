#ifndef ELASTICMEM_KV_RPC_SERVICE_FACTORY_H
#define ELASTICMEM_KV_RPC_SERVICE_FACTORY_H

#include "../block/kv_block.h"
#include "kv_rpc_service.h"

namespace elasticmem {
namespace kv {

class kv_rpc_service_factory : public kv_rpc_serviceIfFactory {
 public:
  explicit kv_rpc_service_factory(std::vector<std::shared_ptr<kv_block>> &blocks);
  kv_rpc_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;
  void releaseHandler(kv_rpc_serviceIf *anIf) override;
 private:
  std::vector<std::shared_ptr<kv_block>> &blocks_;
};

}
}

#endif //ELASTICMEM_KV_RPC_SERVICE_FACTORY_H
