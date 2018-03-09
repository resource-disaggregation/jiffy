#include "kv_service_shard.h"

namespace elasticmem {
namespace kv {

void kv_service_shard::put(const kv_service::key_type &key, const kv_service::value_type &value) {
  entries_.insert(key, value);
}

const kv_service::value_type &kv_service_shard::get(const kv_service::key_type &key) {
  return entries_.find(key);
}

const kv_service::value_type &kv_service_shard::update(const kv_service::key_type &key,
                                                       const kv_service::value_type &value) {
  kv_service::value_type ret = value;
  if (!entries_.update(key, ret)) {
    throw std::out_of_range("No such key [" + key + "]");
  }
  return ret;
}

void kv_service_shard::remove(const kv_service::key_type &key) {
  if (!entries_.erase(key)) {
    throw std::out_of_range("No such key [" + key + "]");
  }
}

void kv_service_shard::clear() {
  entries_.clear();
}

std::size_t kv_service_shard::size() {
  entries_.size(); // TODO: This should return size in bytes...
}

std::size_t kv_service_shard::capacity() {
  entries_.capacity(); // TODO: This should return the capacity in bytes...
}

std::size_t kv_service_shard::num_entries() {
  entries_.size();
}

}
}