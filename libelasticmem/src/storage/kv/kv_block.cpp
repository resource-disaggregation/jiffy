#include "kv_block.h"

namespace elasticmem {
namespace storage {

std::vector<block_op> KV_OPS = {block_op{block_op_type::accessor, "get"},
                                block_op{block_op_type::mutator, "put"},
                                block_op{block_op_type::mutator, "remove"},
                                block_op{block_op_type::mutator, "update"}};

kv_block::kv_block(const std::string& block_name,
                   std::shared_ptr<persistent::persistent_service> persistent,
                   std::string local_storage_prefix,
                   std::shared_ptr<serializer> ser,
                   std::shared_ptr<deserializer> deser)
    : chain_module(block_name, KV_OPS),
      persistent_(std::move(persistent)),
      local_storage_prefix_(std::move(local_storage_prefix)),
      ser_(std::move(ser)),
      deser_(std::move(deser)) {}

void kv_block::put(const key_type &key, const value_type &value) {
  block_.insert(key, value);
}

value_type kv_block::get(const key_type &key) {
  return block_.find(key);
}

void kv_block::update(const key_type &key,
                      const value_type &value) {
  const value_type &ret = value;
  if (!block_.update(key, ret)) {
    throw std::out_of_range("No such key [" + key + "]");
  }
}

void kv_block::remove(const key_type &key) {
  if (!block_.erase(key)) {
    throw std::out_of_range("No such key [" + key + "]");
  }
}

void kv_block::run_command(std::vector<std::string> &_return, int32_t oid, const std::vector<std::string> &args) {
  switch (oid) {
    case 0:
      for (const key_type &key: args)
        _return.push_back(get(key));
      break;
    case 1:
      if (args.size() % 2 != 0) {
        throw std::invalid_argument("Incorrect argument count for put operation: " + std::to_string(args.size()));
      }
      for (size_t i = 0; i < args.size(); i += 2) {
        put(args[i], args[i + 1]);
      }
      _return.emplace_back("OK");
      break;
    case 2:
      for (const key_type &key: args)
        remove(key);
      _return.emplace_back("OK");
      break;
    case 3:
      if (args.size() % 2 != 0) {
        throw std::invalid_argument("Incorrect argument count for update operation: " + std::to_string(args.size()));
      }
      for (size_t i = 0; i < args.size(); i += 2) {
        update(args[i], args[i + 1]);
      }
      _return.emplace_back("OK");
      break;
    default:throw std::invalid_argument("No such operation id " + std::to_string(oid));
  }
}

void kv_block::reset() {
  block_.clear();
  reset_next("nil");
  path("");
}

std::size_t kv_block::size() const {
  return block_.size();
}

bool kv_block::empty() const {
  return block_.empty();
}

void kv_block::load(const std::string &remote_storage_prefix, const std::string &path) {
  locked_hash_table_type ltable = block_.lock_table();
  persistent_->read(remote_storage_prefix + path, local_storage_prefix_ + path);
  deser_->deserialize(local_storage_prefix_ + path, ltable);
  ltable.unlock();
}

void kv_block::flush(const std::string &remote_storage_prefix, const std::string &path) {
  locked_hash_table_type ltable = block_.lock_table();
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