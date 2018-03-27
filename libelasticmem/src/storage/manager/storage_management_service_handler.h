#ifndef ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H
#define ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H

#include "storage_management_service.h"
#include "../block.h"
#include "../chain_module.h"

namespace elasticmem {
namespace storage {

class storage_management_service_handler : public storage_management_serviceIf {
 public:
  explicit storage_management_service_handler(std::vector<std::shared_ptr<chain_module>> &blocks);
  void setup_block(int32_t block_id, const std::string& path, int32_t role, const std::string &next_block_name) override;
  void get_path(std::string &_return, int32_t block_id) override;
  void flush(int32_t block_id, const std::string &persistent_store_prefix, const std::string &path) override;
  void load(int32_t block_id, const std::string &persistent_store_prefix, const std::string &path) override;
  void reset(int32_t block_id) override;
  int64_t storage_capacity(int32_t block_id) override;
  int64_t storage_size(int32_t block_id) override;
  void resend_pending(const int32_t block_id) override;
 private:
  storage_management_exception make_exception(std::exception &e);
  std::vector<std::shared_ptr<chain_module>> &blocks_;
};

}
}

#endif //ELASTICMEM_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H
