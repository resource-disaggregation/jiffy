#include "kv_block.h"
#include "hash_slot.h"
#include "../service/block_chain_client.h"
#include "../../utils/logger.h"
#include "../../directory/fs/directory_client.h"

namespace elasticmem {
namespace storage {

using namespace utils;

std::vector<block_op> KV_OPS = {block_op{block_op_type::accessor, "get"},
                                block_op{block_op_type::accessor, "num_keys"},
                                block_op{block_op_type::mutator, "put"},
                                block_op{block_op_type::mutator, "remove"},
                                block_op{block_op_type::mutator, "update"},
                                block_op{block_op_type::accessor, "zget"},
                                block_op{block_op_type::mutator, "zput"},
                                block_op{block_op_type::mutator, "zremove"},
                                block_op{block_op_type::mutator, "zupdate"}};

const double kv_block::CAPACITY_THRESHOLD = 0.95;

kv_block::kv_block(const std::string &block_name,
                   std::size_t capacity,
                   const std::string &directory_host,
                   int directory_port,
                   std::shared_ptr<persistent::persistent_service> persistent,
                   std::string local_storage_prefix,
                   std::shared_ptr<serializer> ser,
                   std::shared_ptr<deserializer> deser)
    : chain_module(block_name, KV_OPS),
      directory_host_(directory_host),
      directory_port_(directory_port),
      persistent_(std::move(persistent)),
      local_storage_prefix_(std::move(local_storage_prefix)),
      ser_(std::move(ser)),
      deser_(std::move(deser)),
      bytes_(0),
      capacity_(capacity),
      splitting_(false) {}

std::string kv_block::put(const key_type &key, const value_type &value) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash)) {
    bytes_.fetch_add(key.size() + value.size());
    if (state() == block_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    if (block_.insert(key, value)) {
      return "!ok";
    } else {
      return "!duplicate_key";
    }
  }
  LOG(log_level::info) << "Requested key has slot " << hash << " while my slot range is (" << slot_begin() << ", "
                       << slot_end() << ")";
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
      return "!exporting!" + export_target_str();
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
      return "!exporting!" + export_target_str();
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
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

void kv_block::run_command(std::vector<std::string> &_return, int32_t oid, const std::vector<std::string> &args) {
  switch (oid) {
    case kv_op_id::zget:
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
    case kv_op_id::zput:
    case kv_op_id::put:
      if (args.size() % 2 != 0 && args.back() != "!redirected") {
        _return.emplace_back("!args_error");
      } else {
        size_t nargs = static_cast<size_t>(args.size() / 2) * 2;
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(put(args[i], args[i + 1]));
        }
      }
      break;
    case kv_op_id::zremove:
    case kv_op_id::remove:
      for (const key_type &key: args) {
        _return.emplace_back(remove(key));
      }
      break;
    case kv_op_id::zupdate:
    case kv_op_id::update:
      if (args.size() % 2 != 0 && args.back() != "!redirected") {
        _return.emplace_back("!args_error");
      } else {
        size_t nargs = static_cast<size_t>(args.size() / 2) * 2;
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(update(args[i], args[i + 1]));
        }
      }
      break;
    default:throw std::invalid_argument("No such operation id " + std::to_string(oid));
  }
  bool expected = false;
  if (overload() && splitting_.compare_exchange_strong(expected, true)) {
    // Ask directory server to split this slot range
    LOG(log_level::info) << "Overloaded block; storage = " << bytes_.load() << " capacity = " << capacity_
                         << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    directory::directory_client client(directory_host_, directory_port_);
    client.split_slot_range(path(), slot_begin(), slot_end());
    client.disconnect();
    LOG(log_level::info) << "Triggered slot range split";
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
  splitting_ = false;
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
  std::size_t nexport_keys = export_data.size() / 2;
  LOG(log_level::info) << "Exporting " << nexport_keys << " keys ";

  // Add redirected argument so that importing chain does not ignore our request
  export_data.emplace_back("!redirected");

  // Send the data
  block_chain_client dst(export_target());
  auto dst_fut = dst.run_command(kv_op_id::zput, export_data);
  dst_fut.wait();
  dst.disconnect();

  LOG(log_level::info) << "Sent " << nexport_keys << " keys ";

  // Remove our data
  std::vector<std::string> remove_keys;
  export_data.pop_back(); // Remove !redirected argument
  std::size_t nexport_items = export_data.size();
  for (std::size_t i = 0; i < nexport_items; i++) {
    if (i % 2) {
      remove_keys.push_back(export_data.back());
    }
    export_data.pop_back();
  }
  assert(remove_keys.size() == nexport_keys);

  block_chain_client src(chain());
  auto src_fut = src.run_command(kv_op_id::zremove, remove_keys);
  src_fut.wait();
  src.disconnect();

  LOG(log_level::info) << "Removed " << remove_keys.size() << " exported keys";
  bool expected = true;
  if (!splitting_.compare_exchange_strong(expected, false)) {
    LOG(log_level::warn) << "splitting was not true";
  }
}

std::size_t kv_block::storage_capacity() {
  return capacity_;
}

std::size_t kv_block::storage_size() {
  return bytes_.load();
}

bool kv_block::overload() {
  return bytes_.load() > static_cast<size_t>(static_cast<double>(capacity_) * CAPACITY_THRESHOLD);
}

}
}