#include "kv_rpc_service_handler.h"

namespace elasticmem {
namespace storage {

kv_rpc_service_handler::kv_rpc_service_handler(std::vector<std::shared_ptr<kv_block>> &blocks,
                                               std::vector<std::shared_ptr<subscription_map>> &sub_maps)
    : blocks_(blocks), sub_maps_(sub_maps) {}

void kv_rpc_service_handler::put(const int32_t block_id, const std::string &key, const std::string &value) {
  try {
    blocks_.at(static_cast<std::size_t>(block_id))->put(key, value);
    sub_maps_.at(static_cast<std::size_t>(block_id))->notify("put", key);
  } catch (std::out_of_range &e) {
    throw make_exception(e);
  }
}

void kv_rpc_service_handler::get(std::string &_return, const int32_t block_id, const std::string &key) {
  try {
    _return = blocks_.at(static_cast<std::size_t>(block_id))->get(key);
    sub_maps_.at(static_cast<std::size_t>(block_id))->notify("get", key);
  } catch (std::out_of_range &e) {
    throw make_exception(e);
  }
}

void kv_rpc_service_handler::update(const int32_t block_id, const std::string &key, const std::string &value) {
  try {
    blocks_.at(static_cast<std::size_t>(block_id))->update(key, value);
    sub_maps_.at(static_cast<std::size_t>(block_id))->notify("update", key);
  } catch (std::out_of_range &e) {
    throw make_exception(e);
  }
}

void kv_rpc_service_handler::remove(const int32_t block_id, const std::string &key) {
  try {
    blocks_.at(static_cast<std::size_t>(block_id))->remove(key);
    sub_maps_.at(static_cast<std::size_t>(block_id))->notify("remove", key);
  } catch (std::out_of_range &e) {
    throw make_exception(e);
  }
}

int64_t kv_rpc_service_handler::size(const int32_t block_id) {
  try {
    return static_cast<int64_t>(blocks_.at(static_cast<std::size_t>(block_id))->size());
  } catch (std::out_of_range &e) {
    throw make_exception(e);
  }
}

kv_rpc_exception kv_rpc_service_handler::make_exception(const std::out_of_range &ex) {
  kv_rpc_exception e;
  e.msg = ex.what();
  return e;
}

}
}