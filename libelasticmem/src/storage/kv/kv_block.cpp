#include "kv_block.h"

namespace elasticmem {
namespace storage {

std::vector<block_op> KV_OPS = {block_op{block_op_type::accessor, "get"},
                                block_op{block_op_type::mutator, "put"},
                                block_op{block_op_type::mutator, "remove"},
                                block_op{block_op_type::mutator, "update"}};

kv_block::kv_block(const std::string &block_name,
                   std::shared_ptr<persistent::persistent_service> persistent,
                   std::string local_storage_prefix,
                   std::shared_ptr<serializer> ser,
                   std::shared_ptr<deserializer> deser)
    : chain_module(block_name, KV_OPS),
      persistent_(std::move(persistent)),
      local_storage_prefix_(std::move(local_storage_prefix)),
      ser_(std::move(ser)),
      deser_(std::move(deser)),
      bytes_(0) {}

bool kv_block::put(const key_type &key, const value_type &value) {
  bytes_.fetch_add(key.size() + value.size());
  return block_.insert(key, value);
}

value_type kv_block::get(const key_type &key) {
  value_type value;
  if (block_.find(key, value)) {
    return value;
  }
  return "key_not_found";
}

bool kv_block::update(const key_type &key,
                      const value_type &value) {
  return block_.update_fn(key, [&](value_type &v) {
    if (value.size() > v.size()) {
      bytes_.fetch_add(value.size() - v.size());
    } else {
      bytes_.fetch_sub(v.size() - value.size());
    }
    v = value;
  });
}

bool kv_block::remove(const key_type &key) {
  return block_.erase_fn(key, [&](value_type &value) {
    bytes_.fetch_sub(key.size() + value.size());
    return true;
  });
}

void kv_block::run_command(std::vector<std::string> &_return, int32_t oid, const std::vector<std::string> &args) {
  switch (oid) {
    case 0:
      for (const key_type &key: args)
        _return.push_back(get(key));
      break;
    case 1:
      if (args.size() % 2 != 0) {
        _return.emplace_back("args_error");
      } else {
        for (size_t i = 0; i < args.size(); i += 2) {
          if (put(args[i], args[i + 1])) {
            _return.emplace_back("ok");
          } else {
            _return.emplace_back("key_already_exists");
          }
        }
      }
      break;
    case 2:
      for (const key_type &key: args) {
        if (remove(key))
          _return.emplace_back("ok");
        else
          _return.emplace_back("key_not_found");
      }
      break;
    case 3:
      if (args.size() % 2 != 0) {
        _return.emplace_back("args_error");
      } else {
        for (size_t i = 0; i < args.size(); i += 2) {
          if (update(args[i], args[i + 1])) {
            _return.emplace_back("ok");
          } else {
            _return.emplace_back("key_not_found");
          }
        }
      }
      break;
    default:
      throw std::invalid_argument("No such operation id " + std::to_string(oid));
  }
}

void kv_block::reset() {
  block_.clear();
  reset_next_and_listen("nil");
  path("");
  subscriptions().clear();
  clients().clear();
  bytes_.store(0);
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

void kv_block::forward_all() {
  locked_hash_table_type ltable = block_.lock_table();
  int64_t i = 0;
  for (const auto &entry: ltable) {
    std::vector<std::string> result;
    run_command_on_next(result, 1, {entry.first, entry.second});
    ++i;
  }
  ltable.unlock();
}

std::size_t kv_block::storage_capacity() {
  return block_.capacity(); // TODO: This should return the storage_capacity in bytes...
}

std::size_t kv_block::storage_size() {
  return bytes_.load();
}

}
}