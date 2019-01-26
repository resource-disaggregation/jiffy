#include <jiffy/utils/string_utils.h>
#include "hash_table_partition.h"
#include "hash_slot.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"

namespace jiffy {
namespace storage {

using namespace utils;

std::vector<command> KV_OPS = {command{command_type::accessor, "exists"},
                               command{command_type::accessor, "get"},
                               command{command_type::accessor, "keys"},
                               command{command_type::accessor, "num_keys"},
                               command{command_type::mutator, "put"},
                               command{command_type::mutator, "remove"},
                               command{command_type::mutator, "update"},
                               command{command_type::mutator, "lock"},
                               command{command_type::mutator, "unlock"},
                               command{command_type::accessor, "locked_get_data_in_slot_range"},
                               command{command_type::accessor, "locked_get"},
                               command{command_type::mutator, "locked_put"},
                               command{command_type::mutator, "locked_remove"},
                               command{command_type::mutator, "locked_update"},
                               command{command_type::mutator, "upsert"},
                               command{command_type::mutator, "locked_upsert"}};

const int32_t hash_table_partition::SLOT_MAX;

hash_table_partition::hash_table_partition(block_memory_manager *manager,
                                           const std::string &name,
                                           const std::string &metadata,
                                           const utils::property_map &conf)
    : chain_module(manager, name, metadata, KV_OPS),
      block_(HASH_TABLE_DEFAULT_SIZE, hash_type(), equal_type(), build_allocator<kv_pair_type>()),
      locked_block_(block_.lock_table()),
      splitting_(false),
      merging_(false),
      dirty_(false),
      state_(hash_partition_state::regular),
      slot_range_(0, -1),
      export_slot_range_(0, -1),
      import_slot_range_(0, -1) {
  locked_block_.unlock();
  directory_host_ = conf.get("directory.host", "localhost");
  directory_port_ = conf.get_as<int>("directory.port", 9090);
  auto ser = conf.get("hashtable.serializer", "csv");
  if (ser == "binary") {
    ser_ = std::make_shared<csv_serde>();
  } else if (ser == "csv") {
    ser_ = std::make_shared<binary_serde>();
  } else {
    throw std::invalid_argument("No such serializer/deserializer " + ser);
  }
  threshold_hi_ = conf.get_as<double>("hashtable.capacity_threshold_hi", 0.95);
  threshold_lo_ = conf.get_as<double>("hashtable.capacity_threshold_lo", 0.00);
  auto_scale_ = conf.get_as<bool>("hashtable.auto_scale", true);
  LOG(log_level::info) << "Partition name: " << name_;
  auto r = utils::string_utils::split(name_, '_');
  slot_range(std::stoi(r[0]), std::stoi(r[1]));
}

std::string hash_table_partition::put(const key_type &key, const value_type &value, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (state() == hash_partition_state::exporting && in_export_slot_range(hash)) {
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

std::string hash_table_partition::locked_put(const key_type &key, const value_type &value, bool redirect) {
  if (!locked_block_.is_active()) {
    return "!block_not_locked";
  }
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (state() == hash_partition_state::exporting && in_export_slot_range(hash)) {
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

std::string hash_table_partition::upsert(const key_type &key, const value_type &value, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (state() == hash_partition_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    if (block_.upsert(key, [&](value_type &v) {
      if (value.size() > v.size()) {
        bytes_.fetch_add(value.size() - v.size());
      } else {
        bytes_.fetch_sub(v.size() - value.size());
      }
      v = value;
    }, value)) {
      bytes_.fetch_add(key.size() + value.size());
    }
    return "!ok";
  }
  return "!block_moved";
}

std::string hash_table_partition::locked_upsert(const key_type &key, const value_type &value, bool redirect) {
  if (!locked_block_.is_active()) {
    return "!block_not_locked";
  }
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (state() == hash_partition_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    locked_hash_table_type::iterator it;
    if ((it = locked_block_.find(key)) == locked_block_.end()) {
      locked_block_.insert(key, value);
      bytes_.fetch_add(key.size() + value.size());
      return "!ok";
    } else {
      if (value.size() > it->second.size()) {
        bytes_.fetch_add(value.size() - it->second.size());
      } else {
        bytes_.fetch_sub(it->second.size() - value.size());
      }
      locked_block_[key] = value;
    }
    return "!ok";
  }
  return "!block_moved";
}

std::string hash_table_partition::exists(const key_type &key, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (block_.contains(key)) {
      return "true";
    }
    if (state() == hash_partition_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

value_type hash_table_partition::get(const key_type &key, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    value_type value;
    if (block_.find(key, value)) {
      return value;
    }
    if (state() == hash_partition_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string hash_table_partition::locked_get(const key_type &key, bool redirect) {
  if (!locked_block_.is_active()) {
    return "!block_not_locked";
  }
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    auto it = locked_block_.find(key);
    if (it != locked_block_.end()) {
      return it->second;
    }
    if (state() == hash_partition_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string hash_table_partition::update(const key_type &key, const value_type &value, bool redirect) {
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
    if (state() == hash_partition_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string hash_table_partition::locked_update(const key_type &key, const value_type &value, bool redirect) {
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
    if (state() == hash_partition_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string hash_table_partition::remove(const key_type &key, bool redirect) {
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
    if (state() == hash_partition_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string hash_table_partition::locked_remove(const key_type &key, bool redirect) {
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
    if (state() == hash_partition_state::exporting && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

void hash_table_partition::keys(std::vector<std::string> &keys) { // Remove this operation
  if (!locked_block_.is_active()) {
    keys.push_back("!block_not_locked");
    return;
  }
  for (const auto &entry: locked_block_) {
    keys.push_back(entry.first);
  }
}

void hash_table_partition::locked_get_data_in_slot_range(std::vector<std::string> &data,
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

std::string hash_table_partition::unlock() {
  locked_block_.unlock();
  return "!ok";
}

std::string hash_table_partition::lock() {
  locked_block_ = block_.lock_table();
  if (state() == hash_partition_state::exporting) {
    return "!" + export_target_str();
  }
  return "!ok";
}

bool hash_table_partition::is_locked() {
  return locked_block_.is_active();
}

void hash_table_partition::run_command(std::vector<std::string> &_return,
                                       int32_t cmd_id,
                                       const std::vector<std::string> &args) {
  bool redirect = !args.empty() && args.back() == "!redirected";
  size_t nargs = redirect ? args.size() - 1 : args.size();
  switch (cmd_id) {
    case hash_table_cmd_id::exists:
      for (const key_type &key: args)
        _return.push_back(exists(key, redirect));
      break;
    case hash_table_cmd_id::locked_get:
      for (const key_type &key: args)
        _return.emplace_back(locked_get(key, redirect));
      break;
    case hash_table_cmd_id::get:
      for (const key_type &key: args)
        _return.emplace_back(get(key, redirect));
      break;
    case hash_table_cmd_id::num_keys:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(std::to_string(size()));
      }
      break;
    case hash_table_cmd_id::locked_put:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(locked_put(args[i], args[i + 1], redirect));
        }
      }
      break;
    case hash_table_cmd_id::put:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(put(args[i], args[i + 1], redirect));
        }
      }
      break;
    case hash_table_cmd_id::locked_upsert:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(locked_upsert(args[i], args[i + 1], redirect));
        }
      }
      break;
    case hash_table_cmd_id::upsert:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(upsert(args[i], args[i + 1], redirect));
        }
      }
      break;
    case hash_table_cmd_id::locked_remove:
      for (const key_type &key: args) {
        _return.emplace_back(locked_remove(key, redirect));
      }
      break;
    case hash_table_cmd_id::remove:
      for (const key_type &key: args) {
        _return.emplace_back(remove(key, redirect));
      }
      break;
    case hash_table_cmd_id::locked_update:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(locked_update(args[i], args[i + 1], redirect));
        }
      }
      break;
    case hash_table_cmd_id::update:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(update(args[i], args[i + 1], redirect));
        }
      }
      break;
    case hash_table_cmd_id::keys:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        keys(_return);
      }
      break;
    case hash_table_cmd_id::lock:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(lock());
      }
      break;
    case hash_table_cmd_id::unlock:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(unlock());
      }
      break;
    case hash_table_cmd_id::locked_data_in_slot_range:
      if (nargs != 3) {
        _return.emplace_back("!args_error");
      } else {
        locked_get_data_in_slot_range(_return, std::stoi(args[0]), std::stoi(args[1]), std::stoi(args[2]));
      }
      break;
    default:throw std::invalid_argument("No such operation id " + std::to_string(cmd_id));
  }
  if (is_mutator(cmd_id)) {
    dirty_ = true;
  }
  bool expected = false;
  if (auto_scale_.load() && is_mutator(cmd_id) && overload() && state() != hash_partition_state::exporting
      && state() != hash_partition_state::importing && is_tail() && !locked_block_.is_active()
      && splitting_.compare_exchange_strong(expected, true)) {
    // Ask directory server to split this slot range
    LOG(log_level::info) << "Overloaded partition; storage = " << bytes_.load() << " capacity = " << manager_->mb_capacity()
                         << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    try {
      // TODO: Add logic for splitting slot range
      splitting_ = false;
      LOG(log_level::info) << "Requested slot range split";
    } catch (std::exception &e) {
      splitting_ = false;
      LOG(log_level::warn) << "Split slot range failed: " << e.what();
    }
  }
  expected = false;
  if (auto_scale_.load() && cmd_id == hash_table_cmd_id::remove && underload() && state() != hash_partition_state::exporting
      && state() != hash_partition_state::importing && slot_end() != SLOT_MAX && is_tail() && !locked_block_.is_active()
      && merging_.compare_exchange_strong(expected, true)) {
    // Ask directory server to split this slot range
    LOG(log_level::info) << "Underloaded partition; storage = " << bytes_.load() << " capacity = " << manager_->mb_capacity()
                         << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    try {
      // TODO: Add logic for merging slot range
      merging_ = false;
      LOG(log_level::info) << "Requested slot range merge";
    } catch (std::exception &e) {
      merging_ = false;
      LOG(log_level::warn) << "Merge slot range failed: " << e.what();
    }
  }
}

std::size_t hash_table_partition::size() const {
  return block_.size();
}

bool hash_table_partition::empty() const {
  return block_.empty();
}

bool hash_table_partition::is_dirty() const {
  return dirty_.load();
}

void hash_table_partition::load(const std::string &path) {
  locked_hash_table_type ltable = block_.lock_table();
  auto remote = persistent::persistent_store::instance(path, ser_);
  auto decomposed = persistent::persistent_store::decompose_path(path);
  remote->read(decomposed.second, ltable);
  ltable.unlock();
}

bool hash_table_partition::sync(const std::string &path) {
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

bool hash_table_partition::dump(const std::string &path) {
  std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
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
  state_ = hash_partition_state::regular;
  chain_ = {};
  role_ = singleton;
  splitting_ = false;
  merging_ = false;
  dirty_ = false;
  return flushed;
}

void hash_table_partition::forward_all() {
  locked_hash_table_type ltable = block_.lock_table();
  int64_t i = 0;
  for (const auto &entry: ltable) {
    std::vector<std::string> result;
    run_command_on_next(result, hash_table_cmd_id::put, {entry.first, entry.second});
    ++i;
  }
  ltable.unlock();
}

// TODO: Exporting isn't fault tolerant...
void hash_table_partition::export_slots() {
  if (state() != hash_partition_state::exporting) {
    throw std::logic_error("Source partition is not in exporting state");
  }
  auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
  replica_chain_client src(fs, path_, chain(), 0);
  replica_chain_client dst(fs, path_, export_target(), 0);
  auto exp_range = export_slot_range();
  size_t export_batch_size = 1024;
  size_t tot_export_keys = 0;
  bool has_more = true;
  while (has_more) {
    // Lock source and destination blocks
    if (role() == chain_role::singleton) {
      dst.send_command(hash_table_cmd_id::lock, {});
      lock();
      dst.recv_response();
    } else {
      src.send_command(hash_table_cmd_id::lock, {});
      dst.send_command(hash_table_cmd_id::lock, {});
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
        dst.send_command(hash_table_cmd_id::unlock, {});
        unlock();
        dst.recv_response();
      } else {
        src.send_command(hash_table_cmd_id::unlock, {});
        dst.send_command(hash_table_cmd_id::unlock, {});
        src.recv_response();
        dst.recv_response();
      }
      break;
    } else if (export_data.size() < export_batch_size) {  // No more data to export in next iteration
      has_more = false;
    }
    auto nexport_keys = export_data.size() / 2;
    tot_export_keys += nexport_keys;
    LOG(log_level::trace) << "Read " << nexport_keys << " keys to export";

    // Add redirected argument so that importing chain does not ignore our request
    export_data.emplace_back("!redirected");

    // Write data to dst partition
    dst.run_command(hash_table_cmd_id::locked_put, export_data);
    LOG(log_level::trace) << "Sent " << nexport_keys << " keys";

    // Remove data from src partition
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
    src.run_command(hash_table_cmd_id::locked_remove, remove_keys);
    LOG(log_level::trace) << "Removed " << remove_keys.size() << " exported keys";

    // Unlock source and destination blocks
    if (role() == chain_role::singleton) {
      dst.send_command(hash_table_cmd_id::unlock, {});
      unlock();
      dst.recv_response();
    } else {
      src.send_command(hash_table_cmd_id::unlock, {});
      dst.send_command(hash_table_cmd_id::unlock, {});
      src.recv_response();
      dst.recv_response();
    }
  }

  LOG(log_level::info) << "Exported slot range (" << exp_range.first << ", " << exp_range.second << ")";

  splitting_ = false;
  merging_ = false;
}

bool hash_table_partition::overload() {
  return bytes_.load() > static_cast<size_t>(static_cast<double>(manager_->mb_capacity()) * threshold_hi_);
}

bool hash_table_partition::underload() {
  return bytes_.load() < static_cast<size_t>(static_cast<double>(manager_->mb_capacity()) * threshold_lo_);
}

REGISTER_IMPLEMENTATION("hashtable", hash_table_partition);

}
}
