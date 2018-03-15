#include "kv_management_rpc_service_handler.h"

namespace elasticmem {
namespace kv {

kv_management_rpc_service_handler::kv_management_rpc_service_handler(std::vector<std::shared_ptr<kv_block>> &blocks)
    : blocks_(blocks) {}

void kv_management_rpc_service_handler::flush(int32_t block_id,
                                              const std::string &persistent_store_prefix,
                                              const std::string &path) {
  blocks_.at(static_cast<std::size_t>(block_id))->flush(persistent_store_prefix, path);
}

void kv_management_rpc_service_handler::load(int32_t block_id,
                                             const std::string &persistent_store_prefix,
                                             const std::string &path) {
  blocks_.at(static_cast<std::size_t>(block_id))->load(persistent_store_prefix, path);
}

void kv_management_rpc_service_handler::clear(int32_t block_id) {
  blocks_.at(static_cast<std::size_t>(block_id))->clear();
}

int64_t kv_management_rpc_service_handler::storage_capacity(int32_t block_id) {
  return static_cast<int64_t>(blocks_.at(static_cast<std::size_t>(block_id))->storage_capacity());
}

int64_t kv_management_rpc_service_handler::storage_size(int32_t block_id) {
  return static_cast<int64_t>(blocks_.at(static_cast<std::size_t>(block_id))->storage_size());
}

}
}