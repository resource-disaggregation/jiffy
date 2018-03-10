#include "kv_block.h"

namespace elasticmem {
namespace kv {

void kv_block::put(const kv_service::key_type &key, const kv_service::value_type &value) {
  entries_.insert(key, value);
}

const kv_service::value_type &kv_block::get(const kv_service::key_type &key) {
  return entries_.find(key);
}

const kv_service::value_type &kv_block::update(const kv_service::key_type &key,
                                                       const kv_service::value_type &value) {
  kv_service::value_type ret = value;
  if (!entries_.update(key, ret)) {
    throw std::out_of_range("No such key [" + key + "]");
  }
  return ret;
}

void kv_block::remove(const kv_service::key_type &key) {
  if (!entries_.erase(key)) {
    throw std::out_of_range("No such key [" + key + "]");
  }
}

void kv_block::clear() {
  entries_.clear();
}

std::size_t kv_block::size() {
  entries_.size(); // TODO: This should return size in bytes...
}

std::size_t kv_block::storage_capacity() {
  entries_.capacity(); // TODO: This should return the storage_capacity in bytes...
}

std::size_t kv_block::storage_size() {
  entries_.size();
}

}
}