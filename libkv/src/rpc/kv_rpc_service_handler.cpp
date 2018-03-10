#include "kv_rpc_service_handler.h"

namespace elasticmem {
namespace kv {

kv_rpc_service_handler::kv_rpc_service_handler(std::vector<std::shared_ptr<kv_block>> &blocks)
    : blocks_(blocks) {}

void kv_rpc_service_handler::put(const int32_t block_id, const std::string &key, const std::string &value) {
  blocks_.at(static_cast<std::size_t>(block_id))->put(key, value);
}

void kv_rpc_service_handler::get(std::string &_return, const int32_t block_id, const std::string &key) {
  _return.assign(blocks_.at(static_cast<std::size_t>(block_id))->get(key));
}

void kv_rpc_service_handler::update(std::string &_return,
                                    const int32_t block_id,
                                    const std::string &key,
                                    const std::string &value) {
  _return.assign(blocks_.at(static_cast<std::size_t>(block_id))->update(key, value));
}

void kv_rpc_service_handler::remove(const int32_t block_id, const std::string &key) {
  blocks_.at(static_cast<std::size_t>(block_id))->remove(key);
}

}
}