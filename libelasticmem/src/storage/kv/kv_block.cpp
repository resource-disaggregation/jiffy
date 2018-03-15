#include "kv_block.h"

#include <utility>

namespace elasticmem {
namespace storage {

kv_block::kv_block(std::shared_ptr<persistent::persistent_service> persistent,
                   std::string local_storage_prefix,
                   std::shared_ptr<serializer> ser,
                   std::shared_ptr<deserializer> deser)
    : persistent_(std::move(persistent)),
      local_storage_prefix_(std::move(local_storage_prefix)),
      ser_(std::move(ser)), deser_(std::move(deser)) {}

void kv_block::put(const key_type &key, const value_type &value) {
  block_.insert(key, value);
}

value_type kv_block::get(const key_type &key) {
  return block_.find(key);
}

void kv_block::update(const key_type &key,
                      const value_type &value) {
  value_type ret = value;
  if (!block_.update(key, ret)) {
    throw std::out_of_range("No such key [" + key + "]");
  }
}

void kv_block::remove(const key_type &key) {
  if (!block_.erase(key)) {
    throw std::out_of_range("No such key [" + key + "]");
  }
}

void kv_block::clear() {
  block_.clear();
}

std::size_t kv_block::size() const {
  return block_.size();
}

bool kv_block::empty() const {
  return block_.empty();
}

void kv_block::load(const std::string &remote_storage_prefix, const std::string &path) {
  locked_block_type ltable = block_.lock_table();
  persistent_->read(remote_storage_prefix + path, local_storage_prefix_ + path);
  deser_->deserialize(local_storage_prefix_ + path, ltable);
  ltable.unlock();
}

void kv_block::flush(const std::string &remote_storage_prefix, const std::string &path) {
  locked_block_type ltable = block_.lock_table();
  ser_->serialize(ltable, local_storage_prefix_ + path);
  persistent_->write(local_storage_prefix_ + path, remote_storage_prefix + path);
  ltable.clear();
  ltable.unlock();
}

std::size_t kv_block::storage_capacity() {
  return block_.capacity(); // TODO: This should return the storage_capacity in bytes...
}

std::size_t kv_block::storage_size() {
  return block_.size(); // TODO: This should return the storage_size in bytes...
}

}
}