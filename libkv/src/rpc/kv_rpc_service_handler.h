#ifndef ELASTICMEM_KV_RPC_SERVICE_HANDLER_H
#define ELASTICMEM_KV_RPC_SERVICE_HANDLER_H

#include "../block/kv_block.h"
#include "kv_rpc_service.h"

namespace elasticmem {
namespace kv {

class kv_rpc_service_handler: public kv_rpc_serviceIf {
 public:
  explicit kv_rpc_service_handler(std::vector<std::shared_ptr<kv_block>>& blocks);
  void put(int32_t block_id, const std::string &key, const std::string &value) override;
  void get(std::string &_return, int32_t block_id, const std::string &key) override;
  void update(int32_t block_id, const std::string &key, const std::string &value) override;
  void remove(int32_t block_id, const std::string &key) override;
  int64_t size(const int32_t block_id) override;
 private:
  std::vector<std::shared_ptr<kv_block>>& blocks_;
};

}
}

#endif //ELASTICMEM_KV_RPC_SERVICE_HANDLER_H
