#include <jiffy/utils/string_utils.h>
#include <queue>
#include "hash_table_partition.h"
#include "hash_slot.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"
#include "jiffy/auto_scaling/auto_scaling_client.h"
#include <chrono>
#include <thread>

namespace jiffy {
namespace storage {

using namespace utils;

hash_table_partition::hash_table_partition(block_memory_manager *manager,
                                           const std::string &name,
                                           const std::string &metadata,
                                           const utils::property_map &conf,
                                           const std::string &directory_host,
                                           const int directory_port,
                                           const std::string& auto_scaling_host,
                                           const int auto_scaling_port)
    : chain_module(manager, name, metadata, KV_OPS),
      block_(HASH_TABLE_DEFAULT_SIZE, hash_type(), equal_type()),
      splitting_(false),
      merging_(false),
      dirty_(false),
      export_slot_range_(0, -1),
      import_slot_range_(0, -1),
      directory_host_(directory_host),
      directory_port_(directory_port),
      auto_scaling_host_(auto_scaling_host),
      auto_scaling_port_(auto_scaling_port){
  auto ser = conf.get("hashtable.serializer", "csv");
  if (ser == "binary") {
    ser_ = std::make_shared<binary_serde>(binary_allocator_);
  } else if (ser == "csv") {
    ser_ = std::make_shared<csv_serde>(binary_allocator_);
  } else {
    throw std::invalid_argument("No such serializer/deserializer " + ser);
  }
  //threshold_hi_ = conf.get_as<double>("hashtable.capacity_threshold_hi", 0.95);
  threshold_hi_ = 0.7;
  threshold_lo_ = conf.get_as<double>("hashtable.capacity_threshold_lo", 0.05);
  auto_scale_ = conf.get_as<bool>("hashtable.auto_scale", true);
  LOG(log_level::info) << "Partition name: " << name_;
  auto r = utils::string_utils::split(name_, '_');
  slot_range(std::stoi(r[0]), std::stoi(r[1]));
}

std::string hash_table_partition::put(const std::string &key, const std::string &value, bool redirect) {
  LOG(log_level::info) << "Putting key: " << key << " storage_size " << storage_size() << "storage_capacity "
                       << storage_capacity();
  auto hash = hash_slot::get(key);
  LOG(log_level::info) << "Put hash " << hash << " in partition " << name();
  //LOG(log_level::info) << "Putting key: " << key << " Value: " << value << " Hash: " << hash <<" On partition: " << name();
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    if (overload()) {
      LOG(log_level::info) << "this partition is full now !!!!";
      return "!full";
    }
    if (block_.insert(make_binary(key), make_binary(value))) {
      return "!ok";
    } else {
      return "!duplicate_key";
    }
  }
  return "!block_moved";
}

std::string hash_table_partition::upsert(const std::string &key, const std::string &value, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    block_.upsert(make_binary(key), [&](value_type &v) {
      v = make_binary(value);
    }, value, binary_allocator_);
    return "!ok";
  }
  return "!block_moved";
}

std::string hash_table_partition::exists(const std::string &key, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    if (block_.contains(key)) {
      return "true";
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string hash_table_partition::get(const std::string &key, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    try {
      return to_string(block_.find(key));
    } catch (std::out_of_range &e) {
      return "!key_not_found";
    }
  }
  return "!block_moved";
}

std::string hash_table_partition::update(const std::string &key, const std::string &value, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    std::string old_val;
    if (block_.update_fn(key, [&](value_type &v) {
      old_val = to_string(v);
      v = make_binary(value);
    })) {
      LOG(log_level::info) << "successfully removed";
      return old_val;
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string hash_table_partition::remove(const std::string &key, bool redirect) {
  LOG(log_level::info) << "Removing this key" << key << " Size " << storage_size() << " Cap " << storage_capacity() << " name " << name();
  auto hash = hash_slot::get(key);
  LOG(log_level::info) << "Removing hash" << hash;
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    std::string old_val;
    if (block_.erase_fn(key, [&](value_type &value) {
      old_val = to_string(value);
      return true;
    })) {
      return old_val;
    }
    if(metadata_ == "importing") {
      LOG(log_level::info) << "See where it comes from 1";
      return "!block_moved";
    }
    return "!key_not_found";
  }
  LOG(log_level::info) << "See where it comes from 2";
  return "!block_moved";
}

std::string hash_table_partition::scale_remove(const std::string &key) {
  LOG(log_level::info) << "scale removing";
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash)) {
    std::string old_val;
    if (block_.erase_fn(key, [&](value_type &value) {
      old_val = to_string(value);
      return true;
    })) {
      LOG(log_level::info) << "now the storage is " << storage_size();
      return old_val;
    }
    else  LOG(log_level::info) << "Not successful scale remove";
  }
  else throw std::logic_error("this should be in the slot range");
}

void hash_table_partition::keys(std::vector<std::string> &keys) { // Remove this operation
  for (const auto &entry: block_.lock_table()) {
    keys.push_back(to_string(entry.first));
  }
}

bool hash_table_partition::get_data_in_slot_range(std::vector<std::string> &data,
                                                  int32_t slot_begin,
                                                  int32_t slot_end,
                                                  int32_t batch_size) {
  LOG(log_level::info) << "INTO THIS FUNCTION 4 *****************************";
  if(block_.empty()) {
    LOG(log_level::info) << "INTO THIS FUNCTION 5 *****************************";
    return false;
  }
  LOG(log_level::info) << "INTO THIS FUNCTION 6 *****************************";
  std::size_t n_items = 0;
  for (const auto &entry: block_.lock_table()) {
    auto slot = hash_slot::get(entry.first);
    if (slot >= slot_begin && slot <= slot_end) {
      data.push_back(to_string(entry.first));
      data.push_back(to_string(entry.second));
      n_items = n_items + 2;
      LOG(log_level::info) <<"$$$$$$$$$$$$$$$" << n_items;
      if (n_items == static_cast<std::size_t>(batch_size)) {
        return false;
      }
    }
  }
  LOG(log_level::info) << "If the lock table is empty, it should directly get to this line" << data.size();
  return false;
}

std::string hash_table_partition::update_partition(const std::string &new_name, const std::string &new_metadata) {
  LOG(log_level::info) << "Updating partition of " << name() << " to be " << new_name << new_metadata;
  name(new_name);
  auto s = utils::string_utils::split(new_metadata, '$');
  std::string status = s.front();

  if (status == "exporting") {
    export_target(s[2]);
    auto range = utils::string_utils::split(s[1], '_');
    export_slot_range(std::stoi(range[0]), std::stoi(range[1]));
  } else if (status == "importing") {
    auto range = utils::string_utils::split(s[1], '_');
    import_slot_range(std::stoi(range[0]), std::stoi(range[1]));
  } else {
    if (metadata() == "importing") {
      if (import_slot_range().first != slot_range().first || import_slot_range().second != slot_range().second) {
        auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
        fs->remove_block(path(), s[1]);
      }
    } else {
      splitting_ = false;
      merging_ = false;
    }
    export_slot_range(0, -1);
    import_slot_range(0, -1);
  }
  metadata(status);
  slot_range(new_name);
  LOG(log_level::info) << "Partition updated";
  return "!ok";
}

std::vector<std::string> hash_table_partition::get_storage_size() {
  std::vector<std::string> ret;
  ret.emplace_back(std::to_string(storage_size()));
  ret.emplace_back(std::to_string(storage_capacity()));
  return ret;
}

std::string hash_table_partition::get_metadata() {
  return metadata();
}

void hash_table_partition::run_command(std::vector<std::string> &_return,
                                       int32_t cmd_id,
                                       const std::vector<std::string> &args) {
  bool redirect = !args.empty() && args.back() == "!redirected";
  size_t nargs = redirect ? args.size() - 1 : args.size();
  switch (cmd_id) {
    case hash_table_cmd_id::ht_exists:
      for (const std::string &key: args)
        _return.push_back(exists(key, redirect));
      break;
    case hash_table_cmd_id::ht_get:
      for (const std::string &key: args)
        _return.emplace_back(get(key, redirect));
      break;
    case hash_table_cmd_id::ht_num_keys:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(std::to_string(size()));
      }
      break;
    case hash_table_cmd_id::ht_put:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(put(args[i], args[i + 1], redirect));
        }
      }
      break;
    case hash_table_cmd_id::ht_upsert:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(upsert(args[i], args[i + 1], redirect));
        }
      }
      break;
    case hash_table_cmd_id::ht_remove:
      for (size_t i = 0; i < nargs; i += 1) {
        _return.emplace_back(remove(args[i], redirect));
      }
      break;
    case hash_table_cmd_id::ht_update:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(update(args[i], args[i + 1], redirect));
        }
      }
      break;
    case hash_table_cmd_id::ht_keys:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        keys(_return);
      }
      break;
    case hash_table_cmd_id::ht_update_partition:
      if (nargs != 2) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(update_partition(args[0], args[1]));
      }
      break;
    case hash_table_cmd_id::ht_get_storage_size:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        std::vector<std::string> data = get_storage_size();
        _return.insert(_return.end(), data.begin(), data.end());
      }
      break;
    case hash_table_cmd_id::ht_get_metadata:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(get_metadata());
      }
      break;
    case hash_table_cmd_id::ht_get_range_data:
      if (nargs != 3) {
        _return.emplace_back("!args_error");
      } else {
        std::vector<std::string> data;
        auto empty = get_data_in_slot_range(data, std::stoi(args[0]), std::stoi(args[1]), std::stoi(args[2]));
        _return.insert(_return.end(), data.begin(), data.end());
        if(_return.empty())
          _return.emplace_back("!empty");
      }
      break;
    case hash_table_cmd_id::ht_scale_remove:
      for (size_t i = 0; i < nargs; i += 1) {
        _return.emplace_back(scale_remove(args[i]));
      }
      break;
      //TODO no one catches this error
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
                         << storage_capacity()
                         << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    try {
      // TODO: currently the split and merge use similar logic but using redundant coding, should combine them after passing all the test
      splitting_ = true;
      LOG(log_level::info) << "Requested slot range split";
      std::map<std::string, std::string> scale_conf;
      scale_conf.emplace(std::make_pair(std::string("slot_range_begin"), std::to_string(slot_range_.first)));
      scale_conf.emplace(std::make_pair(std::string("slot_range_end"), std::to_string(slot_range_.second)));
      scale_conf.emplace(std::make_pair(std::string("type"), std::string("hash_table_split")));
      auto scale = std::make_shared<auto_scaling::auto_scaling_client>(auto_scaling_host_, auto_scaling_port_);
      scale->auto_scaling(chain(), path(), scale_conf);
    } catch (std::exception &e) {
      splitting_ = false;
      LOG(log_level::warn) << "Split slot range failed: " << e.what();
    }
  }
  expected = false;
  if (auto_scale_.load() && cmd_id == hash_table_cmd_id::ht_remove && underload()
      && metadata_ != "exporting"
      && metadata_ != "importing" && name() != "0_65536" && is_tail()
      && merging_.compare_exchange_strong(expected, true)) {
    // Ask directory server to split this slot range
    LOG(log_level::info) << "Underloaded partition; storage = " << storage_size() << " capacity = "
                         << storage_capacity() << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    try {
      merging_ = true;
      LOG(log_level::info) << "Requested slot range merge";
      std::map<std::string, std::string> scale_conf;
      scale_conf.emplace(std::make_pair(std::string("type"), std::string("hash_table_merge")));
      scale_conf.emplace(std::make_pair(std::string("threshold_hi_"), std::to_string(threshold_hi_)));
      auto scale = std::make_shared<auto_scaling::auto_scaling_client>(auto_scaling_host_, auto_scaling_port_);
      scale->auto_scaling(chain(), path(), scale_conf);
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
  LOG(log_level::info) << "Load happening for path " << path;
  locked_hash_table_type ltable = block_.lock_table();
  auto remote = persistent::persistent_store::instance(path, ser_);
  auto decomposed = persistent::persistent_store::decompose_path(path);
  remote->read<locked_hash_table_type>(decomposed.second, ltable);
  ltable.unlock();
}

bool hash_table_partition::sync(const std::string &path) {
  bool expected = true;
  if (dirty_.compare_exchange_strong(expected, false)) {
    locked_hash_table_type ltable = block_.lock_table();
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<locked_hash_table_type>(ltable, decomposed.second);
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
    remote->write<locked_hash_table_type>(ltable, decomposed.second);
    ltable.unlock();
    flushed = true;
  }
  block_.clear();
  next_->reset("nil");
  path_ = "";
  // clients().clear();
  sub_map_.clear();
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
  LOG(log_level::info) << "In forward all function";
  for (const auto &entry: ltable) {
    std::vector<std::string> result;
    LOG(log_level::info) << "Running put one next partition";
    run_command_on_next(result, hash_table_cmd_id::ht_put, {to_string(entry.first), to_string(entry.second)});
    LOG(log_level::info) << "Successfully put";
    ++i;
  }
  ltable.unlock();
}

// TODO: Exporting isn't fault tolerant...
void hash_table_partition::export_slots() {
  if (state() != hash_partition_state::exporting) {
    throw std::logic_error("Source partition is not in exporting state");
  }
  auto fs =
      std::make_shared<directory::directory_client>(directory_host_, directory_port_); // FIXME: Replace with actual
  replica_chain_client src(fs, path_, chain(), KV_OPS, 0);
  replica_chain_client dst(fs, path_, export_target(), KV_OPS, 0);
  auto exp_range = export_slot_range();
  size_t export_batch_size = 1024;
  size_t tot_export_keys = 0;
  bool has_more = true;
  while (has_more) {
    // Lock source and destination blocks
    // Read data to export
    std::vector<std::string> export_data;
    get_data_in_slot_range(export_data,
                           exp_range.first,
                           exp_range.second,
                           static_cast<int32_t>(export_batch_size));
    if (export_data.empty()) {  // No more data to export
      break;
    } else if (export_data.size() < export_batch_size) {  // No more data to export in next iteration
      has_more = false;
    }
    auto next_port_keys = export_data.size() / 2;
    tot_export_keys += next_port_keys;
    LOG(log_level::trace) << "Read " << next_port_keys << " keys to export";

    // Add redirected argument so that importing chain does not ignore our request
    export_data.emplace_back("!redirected");

    // Write data to dst partition
    dst.run_command(hash_table_cmd_id::ht_put, export_data);
    LOG(log_level::trace) << "Sent " << next_port_keys << " keys";

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
    assert(remove_keys.size() == next_port_keys);
    src.run_command(hash_table_cmd_id::ht_remove, remove_keys);
    LOG(log_level::trace) << "Removed " << remove_keys.size() << " exported keys";
  }

  LOG(log_level::info) << "Exported slot range (" << exp_range.first << ", " << exp_range.second << ")";

  splitting_ = false;
  merging_ = false;
}

bool hash_table_partition::overload() {
  return storage_size() > static_cast<size_t>(static_cast<double>(storage_capacity()) * threshold_hi_);
}

bool hash_table_partition::underload() {
  return storage_size() < static_cast<size_t>(static_cast<double>(storage_capacity()) * threshold_lo_);
}

REGISTER_IMPLEMENTATION("hashtable", hash_table_partition);

}
}
