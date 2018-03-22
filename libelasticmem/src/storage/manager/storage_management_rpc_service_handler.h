#ifndef ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H
#define ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H

#include "storage_management_rpc_service.h"
#include "../block/block.h"

namespace elasticmem {
namespace storage {

class storage_management_rpc_service_handler : public storage_management_rpc_serviceIf {
 public:
  explicit storage_management_rpc_service_handler(std::vector<std::shared_ptr<block>> &blocks);
  void flush(int32_t block_id, const std::string &persistent_store_prefix, const std::string &path) override;
  void load(int32_t block_id, const std::string &persistent_store_prefix, const std::string &path) override;
  void clear(int32_t block_id) override;
  int64_t storage_capacity(int32_t block_id) override;
  int64_t storage_size(int32_t block_id) override;
 private:
  storage_management_rpc_exception make_exception(std::exception &e);
  std::vector<std::shared_ptr<block>> &blocks_;
};

}
}

#endif //ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H
