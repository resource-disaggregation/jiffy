#include <jiffy/utils/string_utils.h>
#include <queue>
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
                                 const int directory_port)
    : chain_module(manager, name, metadata, BTREE_OPS),
      block_(), // TODO currently don't need any further specification
      splitting_(false),
      merging_(false),
      dirty_(false),
      export_slot_range_(0, -1),
      import_slot_range_(0, -1),
      directory_host_(directory_host),
      directory_port_(directory_port) {
  auto ser = conf.get("btree.serializer", "csv");
  if (ser == "binary") {
    ser_ = std::make_shared<csv_serde>();
  } else if (ser == "csv") {
    ser_ = std::make_shared<binary_serde>();
  } else {
    throw std::invalid_argument("No such serializer/deserializer " + ser);
  }
  threshold_hi_ = conf.get_as<double>("btree.capacity_threshold_hi", 0.95);
  threshold_lo_ = conf.get_as<double>("btree.capacity_threshold_lo", 0.00);
  auto_scale_ = conf.get_as<bool>("btree.auto_scale", true);
  LOG(log_level::info) << "Partition name: " << name_;
  std::string min_key = "";
  std::string max_key(MAX_KEY_LENGTH, 0x7f);
  slot_range(min_key, max_key);// TODO deal with name, this is only a hot fix
}

std::string btree_partition::put(const key_type &key, const value_type &value, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
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

std::string btree_partition::exists(const key_type &key, bool redirect) {
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

value_type btree_partition::get(const key_type &key, bool redirect) {
  auto hash = hash_slot::get(key);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && redirect)) {
    value_type value;
    if (block_.find(key, value)) {
      return value;
    }
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string btree_partition::update(const key_type &key, const value_type &value, bool redirect) {
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
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

std::string btree_partition::remove(const key_type &key, bool redirect) {
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
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      return "!exporting!" + export_target_str();
    }
    return "!key_not_found";
  }
  return "!block_moved";
}

void btree_partition::range_lookup(std::vector<std::string> &data,
                                        const key_type begin_range,
                                        const key_type end_range,
                                        bool redirect = false) {
  if (!is_locked()) {
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

std::string btree_partition::update_partition(const std::string new_name, const std::string new_metadata) {
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


std::string btree_partition::get_storage_size() {
  return std::to_string(bytes_.load());
}

std::string btree_partition::get_metadata() {
  return metadata();
}

void btree_partition::run_command(std::vector<std::string> &_return,
                                       int32_t cmd_id,
                                       const std::vector<std::string> &args) {
  bool redirect = !args.empty() && args.back() == "!redirected";
  size_t nargs = redirect ? args.size() - 1 : args.size();
  switch (cmd_id) {
    case b_tree_cmd_id::exists:
      for (const key_type &key: args)
        _return.push_back(exists(key, redirect));
      break;
    case b_tree_cmd_id::get:
      for (const key_type &key: args)
        _return.emplace_back(get(key, redirect));
      break;
    case b_tree_cmd_id::num_keys:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(std::to_string(size()));
      }
      break;
    case b_tree_cmd_id::put:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(put(args[i], args[i + 1], redirect));
        }
      }
      break;
    case b_tree_cmd_id::remove:
      for (const key_type &key: args) {
        _return.emplace_back(remove(key, redirect));
      }
      break;
    case b_tree_cmd_id::update:
      if (args.size() % 2 != 0 && !redirect) {
        _return.emplace_back("!args_error");
      } else {
        for (size_t i = 0; i < nargs; i += 2) {
          _return.emplace_back(update(args[i], args[i + 1], redirect));
        }
      }
      break;
    case b_tree_cmd_id::range_lookup:
      if (nargs != 3) {
        _return.emplace_back("!args_error");
      } else {
        range_lookup(_return, std::stoi(args[0]), std::stoi(args[1]), std::stoi(args[2]));
      }
      break;
      /* TODO need to add auto scaling function in the future
    case b_tree_cmd_id::update_partition:
      if (nargs != 2) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(update_partition(args[0], args[1]));
      }
      break;
    case b_tree_cmd_id::get_storage_size:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(get_storage_size());
      }
      break;
    case b_tree_cmd_id::get_metadata:
      if (nargs != 0) {
        _return.emplace_back("!args_error");
      } else {
        _return.emplace_back(get_metadata());
      }
      break;
*/
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
    LOG(log_level::info) << "Overloaded partition; storage = " << bytes_.load() << " capacity = "
                         << manager_->mb_capacity()
                         << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    try {

      splitting_ = false;
      LOG(log_level::info) << "Not supporting auto_scaling currently";
      /* TODO add auto scaling logic for btree here
      // TODO: currently the split and merge use similar logic but using redundant coding, should combine them after passing all the test
      splitting_ = true;
      LOG(log_level::info) << "Requested slot range split";

      auto split_range_begin = (slot_range_.first + slot_range_.second) / 2;
      auto split_range_end = slot_range_.second;
      std::string dst_partition_name = std::to_string(split_range_begin) + "_" + std::to_string(split_range_end);
      auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
      LOG(log_level::info) << "host !!!!!!!" << directory_host_ << " port " << directory_port_;
      auto dst_replica_chain =
          fs->add_block(path(), dst_partition_name, "importing");

      LOG(log_level::info) << "Look here!!!!!!!";

      // TODO check if add_block succeed, might not be enough capacity in extreme situation
      auto src = std::make_shared<replica_chain_client>(fs, path(), chain(), 0);
      auto dst = std::make_shared<replica_chain_client>(fs, path(), dst_replica_chain, 0);


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
      src->send_command(hash_table_cmd_id::update_partition, src_before_args);
      dst->send_command(hash_table_cmd_id::update_partition, dst_before_args);
      src->recv_response();
      dst->recv_response();
      LOG(log_level::info) << "Look here 1";
      bool has_more = true;
      std::size_t split_batch_size = 1024;
      std::size_t tot_split_keys = 0;
      while (has_more) {
        // Lock source and destination blocks
        if (role() == chain_role::singleton) {
          dst->send_command(hash_table_cmd_id::lock, {});
          lock();
          dst->recv_response();
        } else {
          src->send_command(hash_table_cmd_id::lock, {});
          dst->send_command(hash_table_cmd_id::lock, {});
          src->recv_response();
          dst->recv_response();
        }
        // Read data to split
        std::vector<std::string> split_data;
        locked_get_data_in_slot_range(split_data,
                                      split_range_begin,
                                      split_range_end,
                                      static_cast<int32_t>(split_batch_size));
        if (split_data.size() == 0) {
          if (role() == chain_role::singleton) {
            dst->send_command(hash_table_cmd_id::unlock, {});
            unlock();
            dst->recv_response();
          } else {
            src->send_command(hash_table_cmd_id::unlock, {});
            dst->send_command(hash_table_cmd_id::unlock, {});
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
        dst->run_command(hash_table_cmd_id::locked_put, split_data);
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
        src->run_command(hash_table_cmd_id::locked_remove, remove_keys);
        LOG(log_level::info) << "Removed " << remove_keys.size() << " split keys";

        // Unlock source and destination blocks
        if (role() == chain_role::singleton) {
          dst->send_command(hash_table_cmd_id::unlock, {});
          unlock();
          dst->recv_response();
        } else {
          src->send_command(hash_table_cmd_id::unlock, {});
          dst->send_command(hash_table_cmd_id::unlock, {});
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
      src->send_command(hash_table_cmd_id::update_partition, src_after_args);
      dst->send_command(hash_table_cmd_id::update_partition, dst_after_args);
      src->recv_response();
      dst->recv_response();
      LOG(log_level::info) << "Exported slot range (" << split_range_begin << ", " << split_range_end << ")";
      splitting_ = false;
      merging_ = false;
       */

    } catch (std::exception &e) {
      splitting_ = false;
      LOG(log_level::warn) << "Split slot range failed: " << e.what();
    }
    LOG(log_level::info) << "After split storage: " << manager_->mb_used() << " capacity: " << manager_->mb_capacity();
  }
  expected = false;
  if (auto_scale_.load() && cmd_id == hash_table_cmd_id::remove && underload() && metadata_ != "exporting"
      && metadata_ != "importing" && slot_end() != hash_slot::MAX && is_tail() && !is_locked()
      && merging_.compare_exchange_strong(expected, true)) {
    // Ask directory server to split this slot range
    LOG(log_level::info) << "Underloaded partition; storage = " << bytes_.load() << " capacity = "
                         << manager_->mb_capacity()
                         << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    try {
      merging_ = false;
      LOG(log_level::info) << "Currently does not support auto_scaling";
      // TODO add auto_scaling logic here
      /*
      merging_ = true;
      LOG(log_level::info) << "Requested slot range merge";

      auto merge_range_begin = slot_range_.first;
      auto merge_range_end = slot_range_.second;
      auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
      auto replica_set = fs->dstatus(path()).data_blocks();

      directory::replica_chain merge_target;
      bool able_to_merge = true;
      size_t find_min_size = static_cast<size_t>(static_cast<double>(manager_->mb_capacity())) + 1;
      for (auto &i : replica_set) {
        if (i.fetch_slot_range().first == slot_range().second || i.fetch_slot_range().second == slot_range().first) {
          auto client = std::make_shared<replica_chain_client>(fs, path_, i, 0);
          auto size =
              static_cast<size_t>(std::stoi(client->run_command(hash_table_cmd_id::get_storage_size, {}).front()));
          auto metadata_status = client->run_command(hash_table_cmd_id::get_metadata, {}).front();
          if (size + storage_size() < static_cast<size_t>(static_cast<double>(manager_->mb_capacity()) * threshold_hi_)
              && size < find_min_size) {
            if(metadata_status == "importing" || metadata_status == "exporting")
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
      src->send_command(hash_table_cmd_id::update_partition, src_before_args);
      dst->send_command(hash_table_cmd_id::update_partition, dst_before_args);
      src->recv_response();
      dst->recv_response();

      bool has_more = true;
      std::size_t merge_batch_size = 1024;
      std::size_t tot_merge_keys = 0;
      while (has_more) {
        // Lock source and destination blocks
        if (role() == chain_role::singleton) {
          dst->send_command(hash_table_cmd_id::lock, {});
          lock();
          dst->recv_response();
        } else {
          src->send_command(hash_table_cmd_id::lock, {});
          dst->send_command(hash_table_cmd_id::lock, {});
          src->recv_response();
          dst->recv_response();
        }
        // Read data to merge
        std::vector<std::string> merge_data;
        locked_get_data_in_slot_range(merge_data,
                                      merge_range_begin,
                                      merge_range_end,
                                      static_cast<int32_t>(merge_batch_size));
        if (merge_data.size() == 0) {
          if (role() == chain_role::singleton) {
            dst->send_command(hash_table_cmd_id::unlock, {});
            unlock();
            dst->recv_response();
          } else {
            src->send_command(hash_table_cmd_id::unlock, {});
            dst->send_command(hash_table_cmd_id::unlock, {});
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
        dst->run_command(hash_table_cmd_id::locked_put, merge_data);
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
        src->run_command(hash_table_cmd_id::locked_remove, remove_keys);
        LOG(log_level::trace) << "Removed " << remove_keys.size() << " merged keys";

        // Unlock source and destination blocks
        if (role() == chain_role::singleton) {
          dst->send_command(hash_table_cmd_id::unlock, {});
          unlock();
          dst->recv_response();
        } else {
          src->send_command(hash_table_cmd_id::unlock, {});
          dst->send_command(hash_table_cmd_id::unlock, {});
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
      //src_after_args.push_back(src_partition_name);
      //src_after_args.emplace_back("regular");
      dst_after_args.push_back(dst_partition_name);
      dst_after_args.emplace_back("regular$" + name());
      //src->send_command(hash_table_cmd_id::update_partition, src_after_args);
      dst->send_command(hash_table_cmd_id::update_partition, dst_after_args);
      //src->recv_response();
      dst->recv_response();
      LOG(log_level::info) << "Merged slot range (" << merge_range_begin << ", " << merge_range_end << ")";
      splitting_ = false;
      merging_ = false;
*/
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
  return block_.empty();
}

bool btree_partition::is_dirty() const {
  return dirty_.load();
}

void btree_partition::load(const std::string &path) {
  locked_hash_table_type ltable = block_.lock_table();
  auto remote = persistent::persistent_store::instance(path, ser_);
  auto decomposed = persistent::persistent_store::decompose_path(path);
  remote->read(decomposed.second, ltable);
  ltable.unlock();
}

bool btree_partition::sync(const std::string &path) {
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

bool btree_partition::dump(const std::string &path) {
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

void btree_partition::forward_all() {
  locked_hash_table_type ltable = block_.lock_table();
  int64_t i = 0;
  for (const auto &entry: ltable) {
    std::vector<std::string> result;
    run_command_on_next(result, hash_table_cmd_id::put, {entry.first, entry.second});
    ++i;
  }
  ltable.unlock();
}

bool btree_partition::overload() {
  return manager_->mb_used() > static_cast<size_t>(static_cast<double>(manager_->mb_capacity()) * threshold_hi_);
}

bool btree_partition::underload() {
  return manager_->mb_used() < static_cast<size_t>(static_cast<double>(manager_->mb_capacity()) * threshold_lo_);
}

REGISTER_IMPLEMENTATION("btree", btree_partition);

}
}