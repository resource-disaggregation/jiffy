#ifndef ELASTICMEM_KV_RPC_SERVICE_HANDLER_H
#define ELASTICMEM_KV_RPC_SERVICE_HANDLER_H

#include "kv_block.h"
#include "kv_rpc_service.h"
#include "../notification/subscription_map.h"

namespace elasticmem {
namespace storage {

class kv_rpc_service_handler : public kv_rpc_serviceIf {
 public:
  kv_rpc_service_handler(std::vector<std::shared_ptr<kv_block>> &blocks, std::vector<std::shared_ptr<subscription_map>> &sub_maps);
  void put(int32_t block_id, const std::string &key, const std::string &value) override;
  void get(std::string &_return, int32_t block_id, const std::string &key) override;
  void update(int32_t block_id, const std::string &key, const std::string &value) override;
  void remove(int32_t block_id, const std::string &key) override;
  int64_t size(int32_t block_id) override;
 private:
  kv_rpc_exception make_exception(const std::out_of_range &ex);

  std::vector<std::shared_ptr<kv_block>> &blocks_;
  std::vector<std::shared_ptr<subscription_map>> &sub_maps_;
};

}
}

#endif //ELASTICMEM_KV_RPC_SERVICE_HANDLER_H