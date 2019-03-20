#include <jiffy/utils/string_utils.h>
#include <queue>
#include "hash_table_partition.h"
#include "hash_slot.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"

namespace jiffy {
namespace storage {

using namespace utils;

hash_table_partition::hash_table_partition(block_memory_manager *manager,
                                           const std::string &name,
                                           const std::string &metadata,
                                           const utils::property_map &conf,
                                           const std::string &directory_host,
                                           const int directory_port)
    : chain_module(manager, name, metadata, KV_OPS),
      block_(HASH_TABLE_DEFAULT_SIZE, hash_type(), equal_type(), build_allocator<kv_pair_type>()),
      locked_block_(block_.lock_table()),
      splitting_(false),
      merging_(false),
      dirty_(false),
      export_slot_range_(0, -1),
      import_slot_range_(0, -1),
      directory_host_(directory_host),
      directory_port_(directory_port) {
  locked_block_.unlock();
  auto ser = conf.get("hashtable.serializer", "csv");
  if (ser == "binary") {
    ser_ = std::make_shared<binary_serde>(binary_allocator_);
  } else if (ser == "csv") {
    ser_ = std::make_shared<csv_serde>(binary_allocator_);
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

std::string hash_table_partition::put(const std::string &key, const std::string &value, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    if (block_.insert(make_binary(key), make_binary(value))) {
      return "!ok";
    } else {
      return "!duplicate_key";
    }
  }
  return "!block_moved";
}

std::string hash_table_partition::locked_put(const std::string &key, const std::string &value, bool redirect) {
  if (!is_locked()) {
    return "!block_not_locked";
  }
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    if (locked_block_.insert(make_binary(key), make_binary(value)).second) {
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
    }, reinterpret_cast<const uint8_t *>(value.data()), value.length(), binary_allocator_);
    return "!ok";
  }
  return "!block_moved";
}

std::string hash_table_partition::locked_upsert(const std::string &key, const std::string &value, bool redirect) {
  if (!is_locked()) {
    return "!block_not_locked";
  }
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    locked_hash_table_type::iterator it;
    auto bkey = make_binary(key);
    auto bval = make_binary(value);
    if ((it = locked_block_.find(bkey)) == locked_block_.end()) {
      locked_block_.insert(bkey, bval);
      return "!ok";
    } else {
      locked_block_[bkey] = bval;
    }
    return "!ok";
  }
  return "!block_moved";
}

std::string hash_table_partition::exists(const std::string &key, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (block_.contains(key)) {
      return "true";
    }
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string hash_table_partition::get(const std::string &key, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    try {
      return to_string(block_.find(key));
    } catch (std::out_of_range &e) {
      if (metadata_ == "exporting" && in_export_slot_range(hash)) {
        return "!exporting!" + export_target_str();
      }
      return "!key_not_found";
    }
  }
  return "!block_moved";
}

std::string hash_table_partition::locked_get(const std::string &key, bool redirect) {
  if (!is_locked()) {
    return "!block_not_locked";
  }
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    auto it = locked_block_.find(key);
    if (it != locked_block_.end()) {
      return to_string(it->second);
    }
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string hash_table_partition::update(const std::string &key, const std::string &value, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    std::string old_val;
    if (block_.update_fn(key, [&](value_type &v) {
      old_val = to_string(v);
      v = make_binary(value);
    })) {
      return old_val;
    }
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string hash_table_partition::locked_update(const std::string &key, const std::string &value, bool redirect) {
  if (!is_locked()) {
    return "!block_not_locked";
  }
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    std::string old_val;
    auto it = locked_block_.find(key);
    if (it != locked_block_.end()) {
      old_val = to_string(it->second);
      it->second = make_binary(value);
      return old_val;
    }
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string hash_table_partition::remove(const std::string &key, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    std::string old_val;
    if (block_.erase_fn(key, [&](value_type &value) {
      old_val = to_string(value);
      return true;
    })) {
      return old_val;
    }
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string hash_table_partition::locked_remove(const std::string &key, bool redirect) {
  if (!is_locked()) {
    return "!block_not_locked";
  }
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    std::string old_val;
    auto it = locked_block_.find(key);
    if (it != locked_block_.end()) {
      old_val = to_string(it->second);
      locked_block_.erase(it);
      return old_val;
    }
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

void hash_table_partition::keys(std::vector<std::string> &keys) { // Remove this operation
  if (!is_locked()) {
    keys.emplace_back("!block_not_locked");
    return;
  }
  for (const auto &entry: locked_block_) {
    keys.push_back(to_string(entry.first));
  }
}

void hash_table_partition::locked_get_data_in_slot_range(std::vector<std::string> &data,
                                                         int32_t slot_begin,
                                                         int32_t slot_end,
                                                         int32_t num_keys) {
  if (!is_locked()) {
    data.emplace_back("!block_not_locked");
    return;
  }
  auto n_items = 0;
  for (const auto &entry: locked_block_) {
    auto slot = hash_slot::get(entry.first);
    if (slot >= slot_begin && slot <= slot_end) {
      data.push_back(to_string(entry.first));
      data.push_back(to_string(entry.second));
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
  if (metadata_ == "exporting") {
    return "!" + export_target_str();
  }
  return "!ok";
}

std::string hash_table_partition::update_partition(const std::string &new_name, const std::string &new_metadata) {
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
    }
    export_slot_range(0, -1);
    import_slot_range(0, -1);
  }
  metadata(status);
  return "!ok";
}

std::string hash_table_partition::locked_update_partition(const std::string &new_name,
                                                          const std::string &new_metadata) {
  if (!is_locked()) {
    return "!block_not_locked";
  }
  update(new_name, new_metadata);
  return "!ok";
}

std::string hash_table_partition::get_storage_size() {
  return std::to_string(storage_size());
}

std::string hash_table_partition::locked_get_storage_size() {
  if (!is_locked()) {
    return "!block_not_locked";
  }
  return std::to_string(storage_size());
}

std::string hash_table_partition::get_metadata() {
  return metadata();
}

std::string hash_table_partition::locked_get_metadata() {
  if (!is_locked()) {
    return "!block_not_locked";
  }
  return metadata();
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
    case hash_table_cmd_id::ht_exists:
      for (const std::string &key: args)
        _return.push_back(exists(key, redirect));
      break;
    case hash_table_cmd_id::ht_locked_get:
      for (const std::string &key: args)
        _return.emplace_back(locked_get(key, redirect));
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
    case hash_table_cmd_id::ht_locked_put:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(locked_put(args[i], args[i + 1], redirect));
        }
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
    case hash_table_cmd_id::ht_locked_upsert:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(locked_upsert(args[i], args[i + 1], redirect));
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
    case hash_table_cmd_id::ht_locked_remove:
      for (const std::string &key: args) {
        _return.emplace_back(locked_remove(key, redirect));
      }
      break;
    case hash_table_cmd_id::ht_remove:
      for (const std::string &key: args) {
        _return.emplace_back(remove(key, redirect));
      }
      break;
    case hash_table_cmd_id::ht_locked_update:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(locked_update(args[i], args[i + 1], redirect));
        }
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
    case hash_table_cmd_id::ht_lock:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(lock());
      }
      break;
    case hash_table_cmd_id::ht_unlock:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(unlock());
      }
      break;
    case hash_table_cmd_id::ht_locked_data_in_slot_range:
      if (nargs != 3) {
        _return.emplace_back("!args_error");
      } else {
        locked_get_data_in_slot_range(_return, std::stoi(args[0]), std::stoi(args[1]), std::stoi(args[2]));
      }
      break;
    case hash_table_cmd_id::ht_update_partition:
      if (nargs != 2) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(update_partition(args[0], args[1]));
      }
      break;
    case hash_table_cmd_id::ht_locked_update_partition:
      if (nargs != 2) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(locked_update_partition(args[0], args[1]));
      }
      break;
    case hash_table_cmd_id::ht_get_storage_size:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(get_storage_size());
      }
      break;
    case hash_table_cmd_id::ht_locked_get_storage_size:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(locked_get_storage_size());
      }
      break;
    case hash_table_cmd_id::ht_get_metadata:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(get_metadata());
      }
      break;
    case hash_table_cmd_id::ht_locked_get_metadata:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(locked_get_metadata());
      }
      break;
    default:throw std::invalid_argument("No such operation id " + std::to_string(cmd_id));
  }
  if (is_mutator(cmd_id)) {
    dirty_ = true;
  }
  bool expected = false;
  if (auto_scale_.load() && is_mutator(cmd_id) && overload() && metadata_ != "exporting"
      && metadata_ != "importing" && is_tail() && !is_locked()
      && splitting_.compare_exchange_strong(expected, true)) {
    // Ask directory server to split this slot range
    LOG(log_level::info) << "Overloaded partition; storage = " << storage_size() << " capacity = "
                         << storage_capacity()
                         << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    try {
      // TODO: currently the split and merge use similar logic but using redundant coding, should combine them after passing all the test
      splitting_ = true;
      LOG(log_level::info) << "Requested slot range split";

      auto split_range_begin = (slot_range_.first + slot_range_.second) / 2;
      auto split_range_end = slot_range_.second;
      std::string dst_partition_name = std::to_string(split_range_begin) + "_" + std::to_string(split_range_end);
      auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
      LOG(log_level::info) << "host " << directory_host_ << " port " << directory_port_;
      auto dst_replica_chain =
          fs->add_block(path(), dst_partition_name, "importing");

      LOG(log_level::info) << "Look here!!!!!!!";

      // TODO check if add_block succeed, might not be enough capacity in extreme situation
      auto src = std::make_shared<replica_chain_client>(fs, path(), chain());
      auto dst = std::make_shared<replica_chain_client>(fs, path(), dst_replica_chain);

      std::string src_partition_name = std::to_string(slot_range_.first) + "_" + std::to_string(split_range_begin);
      set_exporting(dst_replica_chain.block_ids,
                    split_range_begin,
                    split_range_end);

      std::vector<std::string> src_before_args;
      std::vector<std::string> dst_before_args;
      src_before_args.push_back(name());
      src_before_args.emplace_back("exporting$" + dst_partition_name + "$" + export_target_str());
      dst_before_args.push_back(dst_partition_name);
      dst_before_args.emplace_back("importing$" + dst_partition_name);
      src->send_command(hash_table_cmd_id::ht_update_partition, src_before_args);
      dst->send_command(hash_table_cmd_id::ht_update_partition, dst_before_args);
      src->recv_response();
      dst->recv_response();
      LOG(log_level::info) << "Look here 1";
      bool has_more = true;
      std::size_t split_batch_size = 1024;
      std::size_t tot_split_keys = 0;
      while (has_more) {
        // Lock source and destination blocks
        if (role() == chain_role::singleton) {
          dst->send_command(hash_table_cmd_id::ht_lock, {});
          lock();
          dst->recv_response();
        } else {
          src->send_command(hash_table_cmd_id::ht_lock, {});
          dst->send_command(hash_table_cmd_id::ht_lock, {});
          src->recv_response();
          dst->recv_response();
        }
        // Read data to split
        std::vector<std::string> split_data;
        locked_get_data_in_slot_range(split_data,
                                      split_range_begin,
                                      split_range_end,
                                      static_cast<int32_t>(split_batch_size));
        if (split_data.empty()) {
          if (role() == chain_role::singleton) {
            dst->send_command(hash_table_cmd_id::ht_unlock, {});
            unlock();
            dst->recv_response();
          } else {
            src->send_command(hash_table_cmd_id::ht_unlock, {});
            dst->send_command(hash_table_cmd_id::ht_unlock, {});
            src->recv_response();
            dst->recv_response();
          }
          break;
        } else if (split_data.size() < split_batch_size) {
          has_more = false;
        }

        LOG(log_level::info) << "Look here 2";
        auto split_keys = split_data.size() / 2;
        tot_split_keys += split_keys;
        LOG(log_level::info) << "Read " << split_keys << " keys to split";

        // Add redirected argument so that importing chain does not ignore our request
        split_data.emplace_back("!redirected");

        // Write data to dst partition
        dst->run_command(hash_table_cmd_id::ht_locked_put, split_data);
        dst->recv_response();
        LOG(log_level::info) << "Sent " << split_keys << " keys";

        // Remove data from src partition
        std::vector<std::string> remove_keys;
        split_data.pop_back(); // Remove !redirected argument
        std::size_t n_split_items = split_data.size();
        for (std::size_t i = 0; i < n_split_items; i++) {
          if (i % 2) {
            remove_keys.push_back(split_data.back());
          }
          split_data.pop_back();
        }
        assert(remove_keys.size() == split_keys);
        LOG(log_level::info) << "Sending " << remove_keys.size() << " split keys to remove";
        src->run_command(hash_table_cmd_id::ht_locked_remove, remove_keys);
        auto ret = src->recv_response();
        for (const auto &x:ret) {
          LOG(log_level::info) << x;
        }
        LOG(log_level::info) << "Removed " << remove_keys.size() << " split keys";

        // Unlock source and destination blocks
        if (role() == chain_role::singleton) {
          dst->send_command(hash_table_cmd_id::ht_unlock, {});
          unlock();
          dst->recv_response();
        } else {
          src->send_command(hash_table_cmd_id::ht_unlock, {});
          dst->send_command(hash_table_cmd_id::ht_unlock, {});
          src->recv_response();
          dst->recv_response();
        }
      }
      // Finalize slot range split
      LOG(log_level::info) << "Look here 3";
      // Update directory mapping
      std::string old_name = name();
      fs->update_partition(path(), old_name, src_partition_name, "regular");

      //Setting name and metadata for src and dst
      std::vector<std::string> src_after_args;
      std::vector<std::string> dst_after_args;
      src_after_args.push_back(src_partition_name);
      src_after_args.emplace_back("regular");
      dst_after_args.push_back(dst_partition_name);
      dst_after_args.emplace_back("regular");
      src->send_command(hash_table_cmd_id::ht_update_partition, src_after_args);
      dst->send_command(hash_table_cmd_id::ht_update_partition, dst_after_args);
      src->recv_response();
      dst->recv_response();
      LOG(log_level::info) << "Exported slot range (" << split_range_begin << ", " << split_range_end << ")";
      splitting_ = false;
      merging_ = false;

    } catch (std::exception &e) {
      splitting_ = false;
      LOG(log_level::warn) << "Split slot range failed: " << e.what();
    }
    LOG(log_level::info) << "After split storage: " << storage_size() << " capacity: " << storage_capacity();
  }
  expected = false;
  if (auto_scale_.load() && cmd_id == hash_table_cmd_id::ht_remove && underload()
      && metadata_ != "exporting"
      && metadata_ != "importing" && slot_end() != hash_slot::MAX && is_tail() && !is_locked()
      && merging_.compare_exchange_strong(expected, true)) {
    // Ask directory server to split this slot range
    LOG(log_level::info) << "Underloaded partition; storage = " << storage_size() << " capacity = "
                         << storage_capacity() << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    try {
      merging_ = true;
      LOG(log_level::info) << "Requested slot range merge";

      auto merge_range_begin = slot_range_.first;
      auto merge_range_end = slot_range_.second;
      auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
      auto replica_set = fs->dstatus(path()).data_blocks();

      directory::replica_chain merge_target;
      bool able_to_merge = true;
      size_t find_min_size = static_cast<size_t>(static_cast<double>(storage_capacity())) + 1;
      for (auto &i : replica_set) {
        if (i.fetch_slot_range().first == slot_range().second || i.fetch_slot_range().second == slot_range().first) {
          auto client = std::make_shared<replica_chain_client>(fs, path_, i, 0);
          auto size =
              static_cast<size_t>(std::stoi(client->run_command(hash_table_cmd_id::ht_get_storage_size,
                                                                {}).front()));
          auto
              metadata_status = client->run_command(hash_table_cmd_id::ht_get_metadata, {}).front();
          if (size + storage_size() < static_cast<size_t>(static_cast<double>(storage_capacity()) * threshold_hi_)
              && size < find_min_size) {
            if (metadata_status == "importing" || metadata_status == "exporting")
              throw std::logic_error("Replica chain already involved in re-partitioning");
            merge_target = i;
            find_min_size = size;
            able_to_merge = false;
          }
        }
      }
      if (able_to_merge) {
        throw std::logic_error("Adjacent partitions are not found or full");
      }

      // Connect two replica chains
      auto src = std::make_shared<replica_chain_client>(fs, path_, chain(), 0);
      auto dst = std::make_shared<replica_chain_client>(fs, path_, merge_target, 0);
      std::string dst_partition_name;
      if (merge_target.fetch_slot_range().first == slot_range().second)
        dst_partition_name =
            std::to_string(slot_range().first) + "_" + std::to_string(merge_target.fetch_slot_range().second);
      else
        dst_partition_name =
            std::to_string(merge_target.fetch_slot_range().first) + "_" + std::to_string(slot_range().second);

      set_exporting(merge_target.block_ids,
                    merge_range_begin,
                    merge_range_end);

      std::vector<std::string> src_before_args;
      std::vector<std::string> dst_before_args;
      src_before_args.push_back(name());
      src_before_args.emplace_back("exporting$" + dst_partition_name + "$" + export_target_str());
      dst_before_args.push_back(merge_target.name);
      dst_before_args.emplace_back("importing$" + name());
      src->send_command(hash_table_cmd_id::ht_update_partition, src_before_args);
      dst->send_command(hash_table_cmd_id::ht_update_partition, dst_before_args);
      src->recv_response();
      dst->recv_response();

      bool has_more = true;
      std::size_t merge_batch_size = 1024;
      std::size_t tot_merge_keys = 0;
      while (has_more) {
        // Lock source and destination blocks
        if (role() == chain_role::singleton) {
          dst->send_command(hash_table_cmd_id::ht_lock, {});
          lock();
          dst->recv_response();
        } else {
          src->send_command(hash_table_cmd_id::ht_lock, {});
          dst->send_command(hash_table_cmd_id::ht_lock, {});
          src->recv_response();
          dst->recv_response();
        }
        // Read data to merge
        std::vector<std::string> merge_data;
        locked_get_data_in_slot_range(merge_data,
                                      merge_range_begin,
                                      merge_range_end,
                                      static_cast<int32_t>(merge_batch_size));
        if (merge_data.empty()) {
          if (role() == chain_role::singleton) {
            dst->send_command(hash_table_cmd_id::ht_unlock, {});
            unlock();
            dst->recv_response();
          } else {
            src->send_command(hash_table_cmd_id::ht_unlock, {});
            dst->send_command(hash_table_cmd_id::ht_unlock, {});
            src->recv_response();
            dst->recv_response();
          }
          break;
        } else if (merge_data.size() < merge_batch_size) {
          has_more = false;
        }

        auto merge_keys = merge_data.size() / 2;
        tot_merge_keys += merge_keys;
        LOG(log_level::trace) << "Read " << merge_keys << " keys to merge";

        // Add redirected argument so that importing chain does not ignore our request
        merge_data.emplace_back("!redirected");

        // Write data to dst partition
        dst->run_command(hash_table_cmd_id::ht_locked_put, merge_data);
        LOG(log_level::trace) << "Sent " << merge_keys << " keys";

        // Remove data from src partition
        std::vector<std::string> remove_keys;
        merge_data.pop_back(); // Remove !redirected argument
        std::size_t n_merge_items = merge_data.size();
        for (std::size_t i = 0; i < n_merge_items; i++) {
          if (i % 2) {
            remove_keys.push_back(merge_data.back());
          }
          merge_data.pop_back();
        }
        assert(remove_keys.size() == merge_keys);
        src->run_command(hash_table_cmd_id::ht_locked_remove, remove_keys);
        LOG(log_level::trace) << "Removed " << remove_keys.size() << " merged keys";

        // Unlock source and destination blocks
        if (role() == chain_role::singleton) {
          dst->send_command(hash_table_cmd_id::ht_unlock, {});
          unlock();
          dst->recv_response();
        } else {
          src->send_command(hash_table_cmd_id::ht_unlock, {});
          dst->send_command(hash_table_cmd_id::ht_unlock, {});
          src->recv_response();
          dst->recv_response();
        }
      }

      // Finalize slot range split

      // Update directory mapping
      fs->update_partition(path(), merge_target.name, dst_partition_name, "regular");

      //Setting name and metadata for src and dst
      std::vector<std::string> src_after_args;
      std::vector<std::string> dst_after_args;
      // We don't need to update the src partition cause it will be deleted anyway
      dst_after_args.push_back(dst_partition_name);
      dst_after_args.emplace_back("regular$" + name());
      dst->send_command(hash_table_cmd_id::ht_update_partition, dst_after_args);
      dst->recv_response();
      LOG(log_level::info) << "Merged slot range (" << merge_range_begin << ", " << merge_range_end << ")";
      splitting_ = false;
      merging_ = false;

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
  for (const auto &entry: ltable) {
    std::vector<std::string> result;
    run_command_on_next(result, hash_table_cmd_id::ht_put, {to_string(entry.first), to_string(entry.second)});
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
  replica_chain_client src(fs, path_, chain(), 0);
  replica_chain_client dst(fs, path_, export_target(), 0);
  auto exp_range = export_slot_range();
  size_t export_batch_size = 1024;
  size_t tot_export_keys = 0;
  bool has_more = true;
  while (has_more) {
    // Lock source and destination blocks
    if (role() == chain_role::singleton) {
      dst.send_command(hash_table_cmd_id::ht_lock, {});
      lock();
      dst.recv_response();
    } else {
      src.send_command(hash_table_cmd_id::ht_lock, {});
      dst.send_command(hash_table_cmd_id::ht_lock, {});
      src.recv_response();
      dst.recv_response();
    }

    // Read data to export
    std::vector<std::string> export_data;
    locked_get_data_in_slot_range(export_data,
                                  exp_range.first,
                                  exp_range.second,
                                  static_cast<int32_t>(export_batch_size));
    if (export_data.empty()) {  // No more data to export
      // Unlock source and destination blocks
      if (role() == chain_role::singleton) {
        dst.send_command(hash_table_cmd_id::ht_unlock, {});
        unlock();
        dst.recv_response();
      } else {
        src.send_command(hash_table_cmd_id::ht_unlock, {});
        dst.send_command(hash_table_cmd_id::ht_unlock, {});
        src.recv_response();
        dst.recv_response();
      }
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
    dst.run_command(hash_table_cmd_id::ht_locked_put, export_data);
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
    src.run_command(hash_table_cmd_id::ht_locked_remove, remove_keys);
    LOG(log_level::trace) << "Removed " << remove_keys.size() << " exported keys";

    // Unlock source and destination blocks
    if (role() == chain_role::singleton) {
      dst.send_command(hash_table_cmd_id::ht_unlock, {});
      unlock();
      dst.recv_response();
    } else {
      src.send_command(hash_table_cmd_id::ht_unlock, {});
      dst.send_command(hash_table_cmd_id::ht_unlock, {});
      src.recv_response();
      dst.recv_response();
    }
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
