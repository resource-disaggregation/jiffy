#include "kv_block.h"
#include "hash_slot.h"
#include "../client/replica_chain_client.h"
#include "../../utils/logger.h"
#include "../../directory/client/directory_client.h"
#include "../../persistent/persistent_store.h"

namespace mmux {
namespace storage {

using namespace utils;

std::vector<block_op> KV_OPS = {block_op{block_op_type::accessor, "exists"},
                                block_op{block_op_type::accessor, "get"},
                                block_op{block_op_type::accessor, "keys"},
                                block_op{block_op_type::accessor, "num_keys"},
                                block_op{block_op_type::mutator, "put"},
                                block_op{block_op_type::mutator, "remove"},
                                block_op{block_op_type::mutator, "update"},
                                block_op{block_op_type::mutator, "lock"},
                                block_op{block_op_type::mutator, "unlock"},
                                block_op{block_op_type::accessor, "locked_get_data_in_slot_range"},
                                block_op{block_op_type::accessor, "locked_get"},
                                block_op{block_op_type::mutator, "locked_put"},
                                block_op{block_op_type::mutator, "locked_remove"},
                                block_op{block_op_type::mutator, "locked_update"}};

kv_block::kv_block(const std::string &block_name,
                   size_t capacity,
                   double threshold_lo,
                   double threshold_hi,
                   const std::string &directory_host,
                   int directory_port,
                   std::shared_ptr<serde> ser)
    : chain_module(block_name, KV_OPS),
      locked_block_(std::move(block_.lock_table())),
      directory_host_(directory_host),
      directory_port_(directory_port),
      ser_(std::move(ser)),
      bytes_(0),
      capacity_(capacity),
      threshold_lo_(threshold_lo),
      threshold_hi_(threshold_hi),
      splitting_(false),
      merging_(false),
      dirty_(false) {
  locked_block_.unlock();
}

std::string kv_block::put(const key_type &key, const value_type &value, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (state() == block_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    if (block_.insert(key, value)) {
      bytes_.fetch_add(key.size() + value.size());
      return "!ok";
    } else {
      return "!duplicate_key";
    }
  }
  return "!block_moved";
}

std::string kv_block::locked_put(const key_type &key, const value_type &value, bool redirect) {
  if (!locked_block_.is_active()) {
    return "!block_not_locked";
  }
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (state() == block_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    if (locked_block_.insert(key, value).second) {
      bytes_.fetch_add(key.size() + value.size());
      return "!ok";
    } else {
      return "!duplicate_key";
    }
  }
  return "!block_moved";
}

std::string kv_block::exists(const key_type &key, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (block_.contains(key)) {
      return "true";
    }
    if (state() == block_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

value_type kv_block::get(const key_type &key, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
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

std::string kv_block::locked_get(const key_type &key, bool redirect) {
  if (!locked_block_.is_active()) {
    return "!block_not_locked";
  }
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    auto it = locked_block_.find(key);
    if (it != locked_block_.end()) {
      return it->second;
    }
    if (state() == block_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string kv_block::update(const key_type &key, const value_type &value, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
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

std::string kv_block::locked_update(const key_type &key, const value_type &value, bool redirect) {
  if (!locked_block_.is_active()) {
    return "!block_not_locked";
  }
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    value_type old_val;
    auto it = locked_block_.find(key);
    if (it != locked_block_.end()) {
      if (value.size() > it->second.size()) {
        bytes_.fetch_add(value.size() - it->second.size());
      } else {
        bytes_.fetch_sub(it->second.size() - value.size());
      }
      old_val = it->second;
      it->second = value;
      return old_val;
    }
    if (state() == block_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string kv_block::remove(const key_type &key, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
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

std::string kv_block::locked_remove(const key_type &key, bool redirect) {
  if (!locked_block_.is_active()) {
    return "!block_not_locked";
  }
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    value_type old_val;
    auto it = locked_block_.find(key);
    if (it != locked_block_.end()) {
      bytes_.fetch_sub(key.size() + it->second.size());
      old_val = it->second;
      locked_block_.erase(it);
      return old_val;
    }
    if (state() == block_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

void kv_block::keys(std::vector<std::string> &keys) { // Remove this operation
  if (!locked_block_.is_active()) {
    keys.push_back("!block_not_locked");
    return;
  }
  for (const auto &entry: locked_block_) {
    keys.push_back(entry.first);
  }
}

void kv_block::locked_get_data_in_slot_range(std::vector<std::string> &data,
                                             int32_t slot_begin,
                                             int32_t slot_end,
                                             int32_t num_keys) {
  if (!locked_block_.is_active()) {
    data.push_back("!block_not_locked");
    return;
  }
  auto n_items = 0;
  for (const auto &entry: locked_block_) {
    auto slot = hash_slot::get(entry.first);
    if (slot >= slot_begin && slot <= slot_end) {
      data.push_back(entry.first);
      data.push_back(entry.second);
      ++n_items;
      if (n_items == num_keys) {
        return;
      }
    }
  }
}

std::string kv_block::unlock() {
  locked_block_.unlock();
  return "!ok";
}

std::string kv_block::lock() {
  locked_block_ = std::move(block_.lock_table());
  if (state() == block_state::exporting) {
    return "!" + export_target_str();
  }
  return "!ok";
}

bool kv_block::is_locked() {
  return locked_block_.is_active();
}

void kv_block::run_command(std::vector<std::string> &_return, int32_t oid, const std::vector<std::string> &args) {
  bool redirect = !args.empty() && args.back() == "!redirected";
  size_t nargs = redirect ? args.size() - 1 : args.size();
  switch (oid) {
    case kv_op_id::exists:
      for (const key_type &key: args)
        _return.push_back(exists(key, redirect));
      break;
    case kv_op_id::locked_get:
      for (const key_type &key: args)
        _return.emplace_back(locked_get(key, redirect));
      break;
    case kv_op_id::get:
      for (const key_type &key: args)
        _return.emplace_back(get(key, redirect));
      break;
    case kv_op_id::num_keys:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(std::to_string(size()));
      }
      break;
    case kv_op_id::locked_put:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(locked_put(args[i], args[i + 1], redirect));
        }
      }
      break;
    case kv_op_id::put:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(put(args[i], args[i + 1], redirect));
        }
      }
      break;
    case kv_op_id::locked_remove:
      for (const key_type &key: args) {
        _return.emplace_back(locked_remove(key, redirect));
      }
      break;
    case kv_op_id::remove:
      for (const key_type &key: args) {
        _return.emplace_back(remove(key, redirect));
      }
      break;
    case kv_op_id::locked_update:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(locked_update(args[i], args[i + 1], redirect));
        }
      }
      break;
    case kv_op_id::update:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(update(args[i], args[i + 1], redirect));
        }
      }
      break;
    case kv_op_id::keys:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        keys(_return);
      }
      break;
    case kv_op_id::lock:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(lock());
      }
      break;
    case kv_op_id::unlock:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(unlock());
      }
      break;
    case kv_op_id::locked_data_in_slot_range:
      if (nargs != 3) {
        _return.emplace_back("!args_error");
      } else {
        locked_get_data_in_slot_range(_return, std::stoi(args[0]), std::stoi(args[1]), std::stoi(args[2]));
      }
      break;
    default:throw std::invalid_argument("No such operation id " + std::to_string(oid));
  }
  if (is_mutator(oid)) {
    dirty_ = true;
  }
  bool expected = false;
  if (auto_scale_.load() && is_mutator(oid) && overload() && state() != block_state::exporting
      && state() != block_state::importing && is_tail() && !locked_block_.is_active()
      && splitting_.compare_exchange_strong(expected, true)) {
    // Ask directory server to split this slot range
    LOG(log_level::info) << "Overloaded block; storage = " << bytes_.load() << " capacity = " << capacity_
                         << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    try {
      directory::directory_client client(directory_host_, directory_port_);
      client.split_slot_range(path(), slot_begin(), slot_end());
      client.disconnect();
      LOG(log_level::info) << "Requested slot range split";
    } catch (std::exception &e) {
      splitting_ = false;
      LOG(log_level::warn) << "Split slot range failed: " << e.what();
    }
  }
  expected = false;
  if (auto_scale_.load() && oid == kv_op_id::remove && underload() && state() != block_state::exporting
      && state() != block_state::importing && slot_end() != block::SLOT_MAX && is_tail() && !locked_block_.is_active()
      && merging_.compare_exchange_strong(expected, true)) {
    // Ask directory server to split this slot range
    LOG(log_level::info) << "Underloaded block; storage = " << bytes_.load() << " capacity = " << capacity_
                         << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    try {
      directory::directory_client client(directory_host_, directory_port_);
      client.merge_slot_range(path(), slot_begin(), slot_end());
      client.disconnect();
      LOG(log_level::info) << "Requested slot range merge";
    } catch (std::exception &e) {
      merging_ = false;
      LOG(log_level::warn) << "Merge slot range failed: " << e.what();
    }
  }
}

void kv_block::reset() {
  std::unique_lock lock(metadata_mtx_);
  block_.clear();
  next_->reset("nil");
  path_ = "";
  // clients().clear();
  sub_map_.clear();
  bytes_.store(0);
  slot_range_.first = 0;
  slot_range_.second = -1;
  state_ = block_state::regular;
  chain_ = {};
  role_ = singleton;
  splitting_ = false;
  merging_ = false;
  dirty_ = false;
}

std::size_t kv_block::size() const {
  return block_.size();
}

bool kv_block::empty() const {
  return block_.empty();
}

bool kv_block::is_dirty() const {
  return dirty_.load();
}

void kv_block::load(const std::string &path) {
  locked_hash_table_type ltable = block_.lock_table();
  auto remote = persistent::persistent_store::instance(path, ser_);
  auto decomposed = persistent::persistent_store::decompose_path(path);
  remote->read(decomposed.second, ltable);
  ltable.unlock();
}

bool kv_block::sync(const std::string &path) {
  bool expected = true;
  if (dirty_.compare_exchange_strong(expected, false)) {
    locked_hash_table_type ltable = block_.lock_table();
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write(ltable, decomposed.second);
    ltable.unlock();
    return true;
  }
  return false;
}

bool kv_block::dump(const std::string &path) {
  std::unique_lock lock(metadata_mtx_);
  bool expected = true;
  bool flushed = false;
  if (dirty_.compare_exchange_strong(expected, false)) {
    locked_hash_table_type ltable = block_.lock_table();
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write(ltable, decomposed.second);
    ltable.unlock();
    flushed = true;
  }
  block_.clear();
  next_->reset("nil");
  path_ = "";
  // clients().clear();
  sub_map_.clear();
  bytes_.store(0);
  slot_range_.first = 0;
  slot_range_.second = -1;
  state_ = block_state::regular;
  chain_ = {};
  role_ = singleton;
  splitting_ = false;
  merging_ = false;
  dirty_ = false;
  return flushed;
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

  replica_chain_client src(chain());
  replica_chain_client dst(export_target());
  auto exp_range = export_slot_range();
  size_t export_batch_size = 1024;
  size_t tot_export_keys = 0;
  bool has_more = true;
  while (has_more) {
    // Lock source and destination blocks
    if (role() == chain_role::singleton) {
      dst.send_command(kv_op_id::lock, {});
      lock();
      dst.recv_response();
    } else {
      src.send_command(kv_op_id::lock, {});
      dst.send_command(kv_op_id::lock, {});
      src.recv_response();
      dst.recv_response();
    }

    // Read data to export
    std::vector<std::string> export_data;
    locked_get_data_in_slot_range(export_data,
                                  exp_range.first,
                                  exp_range.second,
                                  static_cast<int32_t>(export_batch_size));
    if (export_data.size() == 0) {  // No more data to export
      // Unlock source and destination blocks
      if (role() == chain_role::singleton) {
        dst.send_command(kv_op_id::unlock, {});
        unlock();
        dst.recv_response();
      } else {
        src.send_command(kv_op_id::unlock, {});
        dst.send_command(kv_op_id::unlock, {});
        src.recv_response();
        dst.recv_response();
      }
      break;
    } else if (export_data.size() < export_batch_size) {  // No more data to export in next iteration
      has_more = false;
    }
    auto nexport_keys = export_data.size() / 2;
    tot_export_keys += nexport_keys;
    LOG(log_level::info) << "Read " << nexport_keys << " keys to export";

    // Add redirected argument so that importing chain does not ignore our request
    export_data.emplace_back("!redirected");

    // Write data to dst block
    dst.run_command(kv_op_id::locked_put, export_data);
    LOG(log_level::info) << "Sent " << nexport_keys << " keys";

    // Remove data from src block
    std::vector<std::string> remove_keys;
    export_data.pop_back(); // Remove !redirected argument
    std::size_t n_export_items = export_data.size();
    for (std::size_t i = 0; i < n_export_items; i++) {
      if (i % 2) {
        remove_keys.push_back(export_data.back());
      }
      export_data.pop_back();
    }
    assert(remove_keys.size() == nexport_keys);
    src.run_command(kv_op_id::locked_remove, remove_keys);
    LOG(log_level::info) << "Removed " << remove_keys.size() << " exported keys";

    // Unlock source and destination blocks
    if (role() == chain_role::singleton) {
      dst.send_command(kv_op_id::unlock, {});
      unlock();
      dst.recv_response();
    } else {
      src.send_command(kv_op_id::unlock, {});
      dst.send_command(kv_op_id::unlock, {});
      src.recv_response();
      dst.recv_response();
    }
  }

  LOG(log_level::info) << "Completed export for slot range (" << exp_range.first << ", " << exp_range.second << ")";

  splitting_ = false;
  merging_ = false;
}

std::size_t kv_block::storage_capacity() {
  return capacity_;
}

std::size_t kv_block::storage_size() {
  return bytes_.load();
}

bool kv_block::overload() {
  return bytes_.load() > static_cast<size_t>(static_cast<double>(capacity_) * threshold_hi_);
}

bool kv_block::underload() {
  return bytes_.load() < static_cast<size_t>(static_cast<double>(capacity_) * threshold_lo_);
}

}
}