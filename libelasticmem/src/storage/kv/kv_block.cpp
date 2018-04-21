#include "kv_block.h"
#include "hash_slot.h"
#include "../service/block_chain_client.h"

namespace elasticmem {
namespace storage {

std::vector<block_op> KV_OPS = {block_op{block_op_type::accessor, "get"},
                                block_op{block_op_type::accessor, "num_keys"},
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

std::string kv_block::put(const key_type &key, const value_type &value) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash)) {
    bytes_.fetch_add(key.size() + value.size());
    if (state() == block_state::exporting && in_export_slot_range(hash)) {
      return "!exporting:" + export_target_str();
    }
    if (block_.insert(key, value)) {
      return "!ok";
    } else {
      return "!key_not_found";
    }
  }
  return "!block_moved";
}

value_type kv_block::get(const key_type &key) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash)) {
    value_type value;
    if (block_.find(key, value)) {
      return value;
    }
    if (state() == block_state::exporting && in_export_slot_range(hash)) {
      return "!exporting:" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string kv_block::update(const key_type &key, const value_type &value) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash)) {
    value_type old_val;
    if (block_.update_fn(key, [&](value_type &v) {
      if (value.size() > v.size()) {
        bytes_.fetch_add(value.size() - v.size());
      } else {
        bytes_.fetch_sub(v.size() - value.size());
      }
      old_val = v;
      v = value;
    })) {
      return old_val;
    }
    if (state() == block_state::exporting && in_export_slot_range(hash)) {
      return "!exporting:" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string kv_block::remove(const key_type &key) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash)) {
    value_type old_val;
    if (block_.erase_fn(key, [&](value_type &value) {
      bytes_.fetch_sub(key.size() + value.size());
      old_val = value;
      return true;
    })) {
      return old_val;
    }
    if (state() == block_state::exporting && in_export_slot_range(hash)) {
      return "!exporting:" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

void kv_block::run_command(std::vector<std::string> &_return, int32_t oid, const std::vector<std::string> &args) {
  switch (oid) {
    case kv_op_id::get:
      for (const key_type &key: args)
        _return.push_back(get(key));
      break;
    case kv_op_id::num_keys:
      if (!args.empty()) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(std::to_string(size()));
      }
      break;
    case kv_op_id::put:
      if (args.size() % 2 != 0) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < args.size(); i += 2) {
          _return.emplace_back(put(args[i], args[i + 1]));
        }
      }
      break;
    case kv_op_id::remove:
      for (const key_type &key: args) {
        _return.emplace_back(remove(key));
      }
      break;
    case kv_op_id::update:
      if (args.size() % 2 != 0) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < args.size(); i += 2) {
          _return.emplace_back(update(args[i], args[i + 1]));
        }
      }
      break;
    default:throw std::invalid_argument("No such operation id " + std::to_string(oid));
  }
}

// TODO: This should be atomic...
void kv_block::reset() {
  block_.clear();
  reset_next_and_listen("nil");
  path("");
  subscriptions().clear();
  clients().clear();
  bytes_.store(0);
  slot_range(0, -1);
  state(block_state::regular);
  chain({});
  role(singleton);
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
    run_command_on_next(result, kv_op_id::put, {entry.first, entry.second});
    ++i;
  }
  ltable.unlock();
}

// TODO: Exporting isn't fault tolerant...
void kv_block::export_slots() {
  if (state() != block_state::exporting) {
    throw std::logic_error("Source block is not in exporting state");
  }

  // TODO: This should be obtainable without acquiring locks...
  // Extract the data
  std::vector<std::string> export_data;
  locked_hash_table_type ltable = block_.lock_table();
  for (const auto &entry: ltable) {
    if (in_export_slot_range(hash_slot::get(entry.first))) {
      export_data.push_back(entry.first);
      export_data.push_back(entry.second);
    }
  }
  ltable.unlock();

  // Send the data
  block_chain_client dst(export_target());
  auto dst_fut = dst.run_command(kv_op_id::put, export_data);
  dst_fut.wait();
  dst.disconnect();

  // Remove our data
  std::vector<std::string> remove_keys;
  for (std::size_t i = 0; i < export_data.size(); i++) {
    if (i % 2) {
      remove_keys.push_back(export_data.back());
    }
    remove_keys.pop_back();
  }
  block_chain_client src(chain());
  auto src_fut = dst.run_command(kv_op_id::remove, remove_keys);
  src_fut.wait();
}

std::size_t kv_block::storage_capacity() {
  return block_.capacity(); // TODO: This should return the storage_capacity in bytes...
}

std::size_t kv_block::storage_size() {
  return bytes_.load();
}

}
}