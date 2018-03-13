#ifndef ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H
#define ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H

#include "../block/kv_block.h"
#include "kv_management_rpc_service.h"

namespace elasticmem {
namespace kv {

class kv_management_rpc_service_handler: public kv_management_rpc_serviceIf {
 public:
  explicit kv_management_rpc_service_handler(std::vector<std::shared_ptr<kv_block>>& blocks);
  void flush(int32_t block_id, const std::string& persistent_store_prefix, const std::string &path) override;
  void load(int32_t block_id, const std::string& persistent_store_prefix, const std::string &path) override;
  void clear(int32_t block_id) override;
  int64_t storage_capacity(int32_t block_id) override;
  int64_t storage_size(int32_t block_id) override;
 private:
  std::vector<std::shared_ptr<kv_block>>& blocks_;
};

}
}

#endif //ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H
