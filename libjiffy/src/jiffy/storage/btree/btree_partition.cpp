#include <jiffy/utils/string_utils.h>
#include "btree_partition.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"
#include "jiffy/storage/btree/btree_ops.h"
#include "jiffy/directory/client/directory_client.h"
#include "jiffy/directory/directory_ops.h"

namespace jiffy {
namespace storage {

using namespace utils;

btree_partition::btree_partition(block_memory_manager *manager,
                                 const std::string &name,
                                 const std::string &metadata,
                                 const utils::property_map &conf,
                                 const std::string &directory_host,
                                 const int directory_port,
                                 const std::string &auto_scaling_host,
                                 const int auto_scaling_port)
    : chain_module(manager, name, metadata, BTREE_OPS),
      partition_(less_type()),
      splitting_(false),
      merging_(false),
      dirty_(false),
      directory_host_(directory_host),
      directory_port_(directory_port),
      auto_scaling_host_(auto_scaling_host),
      auto_scaling_port_(auto_scaling_port) {
  auto ser = conf.get("btree.serializer", "csv");
  if (ser == "binary") {
    ser_ = std::make_shared<csv_serde>(binary_allocator_);
  } else if (ser == "csv") {
    ser_ = std::make_shared<binary_serde>(binary_allocator_);
  } else {
    throw std::invalid_argument("No such serializer/deserializer " + ser);
  }
  threshold_hi_ = conf.get_as<double>("btree.capacity_threshold_hi", 0.95);
  threshold_lo_ = conf.get_as<double>("btree.capacity_threshold_lo", 0.00);
  auto_scale_ = conf.get_as<bool>("btree.auto_scale", true);
  LOG(log_level::info) << "Partition name: " << name_;
  slot_range(MIN_KEY, MAX_KEY);// TODO deal with name, this is only a hot fix
}

std::string btree_partition::put(const std::string &key, const std::string &value, bool redirect) {
  LOG(log_level::info) << "Putting " << key << " with storage size " << storage_size() << " and capacity " << storage_capacity();
  if (in_slot_range(key) || (in_import_slot_range(key) && redirect)) {
    if (metadata_ == "exporting" && in_export_slot_range(key)) {
      return "!exporting!" + export_target_str();
    }
    LOG(log_level::info) << "Putting 1";
    auto k = make_binary(key);
    LOG(log_level::info) << "Putting 2";
    auto v = make_binary(value);
    LOG(log_level::info) << "Putting 3";
    auto p = std::make_pair(k, v);
    LOG(log_level::info) << "Putting 4";
    if (partition_.insert(p).second) {
      LOG(log_level::info) << "Putting 5";
      return "!ok";
    } else {
      LOG(log_level::info) << "Putting 6";
      return "!duplicate_key";
    }
  }
  return "!block_moved";
}

std::string btree_partition::exists(const std::string &key, bool redirect) {
  if (in_slot_range(key) || (in_import_slot_range(key) && redirect)) {
    if (partition_.find(make_binary(key)) != partition_.end()) {
      return "true";
    }
    if (metadata_ == "exporting" && in_export_slot_range(key)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string btree_partition::get(const std::string &key, bool redirect) {
  if (in_slot_range(key) || (in_import_slot_range(key) && redirect)) {
    value_type value;
    auto ret = partition_.find(make_binary(key));
    if (ret != partition_.end()) {
      return to_string(ret->second);
    }
    if (metadata_ == "exporting" && in_export_slot_range(key)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string btree_partition::update(const std::string &key, const std::string &value, bool redirect) {
  if (in_slot_range(key) || (in_import_slot_range(key) && redirect)) {
    std::string old_val;
    auto bkey = make_binary(key);
    auto ret = partition_.find(bkey);
    if (ret != partition_.end()) {
      old_val = to_string(ret->second);
      partition_.erase(ret);
      partition_.insert(std::make_pair(bkey, make_binary(value)));//TODO fix this
      return old_val;
    }
    if (metadata_ == "exporting" && in_export_slot_range(key)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string btree_partition::remove(const std::string &key, bool redirect) {
  if (in_slot_range(key) || (in_import_slot_range(key) && redirect)) {
    std::string old_val;
    auto bkey = make_binary(key);
    auto ret = partition_.find(bkey);
    if (ret != partition_.end()) {
      old_val = to_string(ret->second);
      partition_.erase(bkey);
      return old_val;
    }
    if (metadata_ == "exporting" && in_export_slot_range(key)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::vector<std::string> btree_partition::range_lookup(const std::string& begin_range,
                                                       const std::string& end_range,
                                                       bool redirect) {
  std::vector<std::string> result;
  //if (begin_range > end_range) return "!ok"; // TODO fix this
  auto start = partition_.lower_bound(make_binary(begin_range));
  auto edge = partition_.upper_bound(make_binary(end_range));
  auto end = std::prev(edge);
  if ((end->first >= slot_range_.first && start->first <= slot_range_.second)
      || (end->first >= std::min(import_slot_range_.first, slot_range_.first)
          && start->first <= std::max(import_slot_range_.second, slot_range_.second) && redirect)) {
    for (auto entry = start; entry != edge; entry++) {
      if (to_string(entry->first) >= begin_range && to_string(entry->first) <= end_range) {
        result.push_back(to_string(entry->first));
        result.push_back(to_string(entry->second));
      }
    }
    return result;
  }
  return std::vector<std::string>{"!Incorrect key range"};
}

std::string btree_partition::range_count(const std::string& begin_range,
                                         const std::string& end_range,
                                         bool redirect) {
  auto start = partition_.lower_bound(make_binary(begin_range));
  auto edge = partition_.upper_bound(make_binary(end_range));
  auto end = std::prev(edge);
  if ((end.key() >= slot_range_.first && start.key() <= slot_range_.second)
      || (end.key() >= min(import_slot_range_.first, slot_range_.first)
          && start.key() <= max(import_slot_range_.second, slot_range_.second) && redirect)) {
    std::size_t ret = 0;
    for (auto entry = start; entry != edge; entry++) {
      if (to_string(entry->first) >= begin_range && to_string(entry->first) <= end_range) {
        ret++;
      }
    }
    return std::to_string(ret);
  }
  return std::string{"!Incorrect key range"};
}

void btree_partition::run_command(std::vector<std::string> &_return,
                                  int32_t cmd_id,
                                  const std::vector<std::string> &args) {
  bool redirect = !args.empty() && args.back() == "!redirected";
  size_t nargs = redirect ? args.size() - 1 : args.size();
  switch (cmd_id) {
    case btree_cmd_id::bt_exists:
      for (const std::string &key: args)
        _return.push_back(exists(key, redirect));
      break;
    case btree_cmd_id::bt_get:
      for (const std::string &key: args)
        _return.emplace_back(get(key, redirect));
      break;
    case btree_cmd_id::bt_num_keys:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(std::to_string(size()));
      }
      break;
    case btree_cmd_id::bt_put:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(put(args[i], args[i + 1], redirect));
        }
      }
      break;
    case btree_cmd_id::bt_remove:
      for (const std::string &key: args) {
        _return.emplace_back(remove(key, redirect));
      }
      break;
    case btree_cmd_id::bt_update:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(update(args[i], args[i + 1], redirect));
        }
      }
      break;
    case btree_cmd_id::bt_range_lookup:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          std::vector<std::string> result = range_lookup(args[i], args[i + 1], redirect);
          _return.insert(_return.end(), result.begin(), result.end());
        }
      }
      break;
    case btree_cmd_id::bt_range_count:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(range_count(args[i], args[i + 1], redirect));
        }
      }
      break;
    default:throw std::invalid_argument("No such operation id " + std::to_string(cmd_id));
  }
  if (is_mutator(cmd_id)) {
    dirty_ = true;
  }
  bool expected = false;
  if (auto_scale_.load() && is_mutator(cmd_id) && overload() && metadata_ != "exporting"
      && metadata_ != "importing" && is_tail()
      && splitting_.compare_exchange_strong(expected, true)) {
    // Ask directory server to split this slot range
    LOG(log_level::info) << "Overloaded partition; storage = " << storage_size() << " capacity = "
                         << storage_capacity() << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    try {

      splitting_ = false;
      LOG(log_level::info) << "Not supporting auto_scaling currently";

    } catch (std::exception &e) {
      splitting_ = false;
      LOG(log_level::warn) << "Split slot range failed: " << e.what();
    }
    LOG(log_level::info) << "After split storage: " << storage_size() << " capacity: " << storage_size();
  }
  expected = false;
  if (auto_scale_.load() && cmd_id == btree_cmd_id::bt_remove && underload()
      && metadata_ != "exporting"
      && metadata_ != "importing" && slot_end() != MAX_KEY && is_tail()
      && merging_.compare_exchange_strong(expected, true)) {
    // Ask directory server to split this slot range
    LOG(log_level::info) << "Underloaded partition; storage = " << storage_size() << " capacity = "
                         << storage_capacity() << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    try {
      merging_ = false;
      LOG(log_level::info) << "Currently does not support auto_scaling";
    } catch (std::exception &e) {
      merging_ = false;
      LOG(log_level::warn) << "Merge slot range failed: " << e.what();
    }
  }
}

std::size_t btree_partition::size() const {
  return partition_.size();
}

bool btree_partition::empty() const {
  return partition_.empty();
}

bool btree_partition::is_dirty() const {
  return dirty_.load();
}

void btree_partition::load(const std::string &path) {
  auto remote = persistent::persistent_store::instance(path, ser_);
  auto decomposed = persistent::persistent_store::decompose_path(path);
  remote->read<btree_type>(decomposed.second, partition_);
}

bool btree_partition::sync(const std::string &path) {
  bool expected = true;
  if (dirty_.compare_exchange_strong(expected, false)) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<btree_type>(partition_, decomposed.second);
    return true;
  }
  return false;
}

bool btree_partition::dump(const std::string &path) {
  std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
  bool expected = true;
  bool flushed = false;
  if (dirty_.compare_exchange_strong(expected, false)) {
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<btree_type>(partition_, decomposed.second);
    flushed = true;
  }
  partition_.clear();
  next_->reset("nil");
  path_ = "";
  // clients().clear();
  sub_map_.clear();
  // slot_range_.first = ""; TODO fix this
  // slot_range_.second = "";
  chain_ = {};
  role_ = singleton;
  splitting_ = false;
  merging_ = false;
  dirty_ = false;
  return flushed;
}

void btree_partition::forward_all() {
  int64_t i = 0;
  for (auto it = partition_.begin(); it != partition_.end(); it++) {
    std::vector<std::string> result;
    run_command_on_next(result, btree_cmd_id::bt_put, {to_string(it->first), to_string(it->second)});
    ++i;
  }
}

bool btree_partition::overload() {
  return storage_size() > static_cast<size_t>(static_cast<double>(storage_capacity()) * threshold_hi_);
}

bool btree_partition::underload() {
  return storage_size() < static_cast<size_t>(static_cast<double>(storage_capacity()) * threshold_lo_);
}

REGISTER_IMPLEMENTATION("btree", btree_partition);

}
}