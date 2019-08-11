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
                                           int directory_port,
                                           const std::string &auto_scaling_host,
                                           int auto_scaling_port)
    : chain_module(manager, name, metadata, HT_OPS),
      block_(HASH_TABLE_DEFAULT_SIZE, hash_type(), equal_type()),
      scaling_up_(false),
      scaling_down_(false),
      dirty_(false),
      state_(regular),
      export_slot_range_(0, -1),
      import_slot_range_(0, -1),
      directory_host_(directory_host),
      directory_port_(directory_port),
      auto_scaling_host_(auto_scaling_host),
      auto_scaling_port_(auto_scaling_port) {
  auto ser = conf.get("hashtable.serializer", "csv");
  if (ser == "binary") {
    ser_ = std::make_shared<binary_serde>(binary_allocator_);
  } else if (ser == "csv") {
    ser_ = std::make_shared<csv_serde>(binary_allocator_);
  } else {
    throw std::invalid_argument("No such serializer/deserializer " + ser);
  }
  threshold_hi_ = conf.get_as<double>("hashtable.capacity_threshold_hi", 0.95);
  threshold_lo_ = conf.get_as<double>("hashtable.capacity_threshold_lo", 0.05);
  auto_scale_ = conf.get_as<bool>("hashtable.auto_scale", true);
  auto r = utils::string_utils::split(name_, '_');
  slot_range(std::stoi(r[0]), std::stoi(r[1]));
}

void hash_table_partition::put(response &_return, const arg_list &args) {
  if (!(args.size() == 3 || (args.size() == 4 && args[3] == "!redirected"))) {
    RETURN_ERR("!args_error");
  }
  auto hash = hash_slot::get(args[1]);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && args[3] == "!redirected")) {
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      RETURN_ERR("!exporting", export_target_str_);
    }
    if (overload()) {
      RETURN_ERR("!full");
    }

    if (block_.insert(make_binary(args[1]), make_binary(args[2]))) {
      RETURN_OK();
    } else {
      RETURN_ERR("!duplicate_key");
    }
  }
  RETURN_ERR("!block_moved");
}

void hash_table_partition::upsert(response &_return, const arg_list &args) {
  if (!(args.size() == 3 || (args.size() == 4 && args[3] == "!redirected"))) {
    RETURN_ERR("!args_error");
  }
  auto hash = hash_slot::get(args[1]);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && args[3] == "!redirected")) {
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      RETURN_ERR("!exporting", export_target_str_);
    }
    block_.upsert(make_binary(args[1]), [&](value_type &v) {
      v = make_binary(args[2]);
    }, args[2], binary_allocator_);
    RETURN_OK();
  }
  RETURN_ERR("!block_moved");
}

void hash_table_partition::exists(response &_return, const arg_list &args) {
  if (!(args.size() == 2 || (args.size() == 3 && args[2] == "!redirected"))) {
    RETURN_ERR("!args_error");
  }
  auto hash = hash_slot::get(args[1]);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && args[2] == "!redirected")) {
    if (block_.contains(args[1])) {
      RETURN_OK("true");
    }
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      RETURN_ERR("!exporting", export_target_str_);
    }
    RETURN_OK("false");
  }
  RETURN_ERR("!block_moved");
}

void hash_table_partition::get(response &_return, const arg_list &args) {
  if (!(args.size() == 2 || (args.size() == 3 && args[2] == "!redirected"))) {
    RETURN("!args_error");
  }
  auto hash = hash_slot::get(args[1]);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && args[2] == "!redirected")) {
    try {
      RETURN_OK(to_string(block_.find(args[1])));
    } catch (std::out_of_range &e) {
      if (metadata_ == "exporting" && in_export_slot_range(hash)) {
        RETURN_ERR("!exporting", export_target_str_);
      }
      RETURN_ERR("!key_not_found");
    }
  }
  RETURN_ERR("!block_moved");
}

void hash_table_partition::update(response &_return, const arg_list &args) {
  if (!(args.size() == 3 || (args.size() == 4 && args[3] == "!redirected"))) {
    RETURN_ERR("!args_error");
  }
  auto hash = hash_slot::get(args[1]);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && args[3] == "!redirected")) {
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      RETURN_ERR("!exporting", export_target_str_);
    }
    std::string old_val;
    if (block_.update_fn(args[1], [&](value_type &v) {
      old_val = to_string(v);
      v = make_binary(args[2]);
    })) {
      RETURN_OK(old_val);
    }
    if (metadata_ == "importing" && in_import_slot_range(hash)) {
      RETURN_ERR("!full");
    }
    RETURN_ERR("!key_not_found");
  }
  RETURN_ERR("!block_moved");
}

void hash_table_partition::remove(response &_return, const arg_list &args) {
  if (!(args.size() == 2 || (args.size() == 3 && args[2] == "!redirected"))) {
    RETURN_ERR("!args_error");
  }
  auto hash = hash_slot::get(args[1]);
  if (in_slot_range(hash) || (in_import_slot_range(hash) && args[2] == "!redirected")) {
    if (metadata_ == "exporting" && in_export_slot_range(hash)) {
      RETURN_ERR("!exporting", export_target_str_);
    }
    std::string old_val;
    if (block_.erase_fn(args[1], [&](value_type &value) {
      old_val = to_string(value);
      return true;
    })) {
      RETURN_OK(old_val);
    }
    if (metadata_ == "importing" && in_import_slot_range(hash)) {
      RETURN_ERR("!full");
    }
    RETURN_ERR("!key_not_found");
  }
  RETURN_ERR("!block_moved");
}

void hash_table_partition::scale_remove(response &_return, const arg_list &args) {
  for (size_t i = 1; i < args.size(); ++i) {
    if (!block_.erase(args[i])) {
      LOG(log_level::error) << "Unsuccessful scale remove";
    }
  }
  RETURN_OK();
}

void hash_table_partition::scale_put(response &_return, const arg_list &args) {
  for (size_t i = 1; i < args.size(); i += 2) {
    if (!block_.insert(make_binary(args[i]), make_binary(args[i + 1]))) {
      LOG(log_level::error) << "Unsuccessful scale put";
    }
  }
  RETURN_OK();
}

void hash_table_partition::get_data_in_slot_range(response &_return, const arg_list &args) {
  if (args.size() != 4) {
    RETURN_ERR("!args_error");
  }
  std::size_t n_items = 0;
  auto slot_begin = std::stoi(args[1]);
  auto slot_end = std::stoi(args[2]);
  auto batch_size = std::stoull(args[3]);
  for (const auto &entry: block_.lock_table()) {
    auto slot = hash_slot::get(entry.first);
    if (slot >= slot_begin && slot < slot_end) {
      if (_return.empty())
        _return.emplace_back("!ok");
      _return.emplace_back(to_string(entry.first));
      _return.emplace_back(to_string(entry.second));
      n_items += 2;
      if (n_items == static_cast<std::size_t>(batch_size)) {
        return;
      }
    }
  }
  if (_return.empty()) {
    RETURN_ERR("!empty");
  }
}

void hash_table_partition::update_partition(response &_return, const arg_list &args) {
  if (args.size() != 3) {
    RETURN_ERR("!args_error");
  }
  update_lock_.lock();
  auto new_name = args[1];
  auto new_metadata = args[2];
  if (new_name == "merging" && new_metadata == "merging") {
    if (metadata() == "regular" && name() != "0_65536") {
      metadata("exporting");
      update_lock_.unlock();
      RETURN_OK(name());
    }
    scaling_up_ = false;
    scaling_down_ = false;
    update_lock_.unlock();
    RETURN_ERR("!fail");
  }
  auto s = utils::string_utils::split(new_metadata, '$');
  std::string status = s.front();
  if (status == "exporting") {
    // When we meet exporting, the original state must be regular
    export_target(s[2]);
    auto range = utils::string_utils::split(s[1], '_');
    export_slot_range(std::stoi(range[0]), std::stoi(range[1]));
  } else if (status == "importing") {
    if (metadata() != "regular" && metadata() != "split_importing") {
      update_lock_.unlock();
      RETURN_ERR("!fail");
    }
    auto range = utils::string_utils::split(s[1], '_');
    import_slot_range(std::stoi(range[0]), std::stoi(range[1]));
  } else {
    if (metadata() == "importing") {
      if ((import_slot_range().first != slot_range().first || import_slot_range().second != slot_range().second)
          && is_tail()) {
        auto fs = std::make_shared<directory::directory_client>(directory_host_, directory_port_);
        fs->remove_block(path(), s[1]);
      }
      if (!underload()) {
        scaling_down_ = false;
      }
    } else {
      scaling_up_ = false;
      scaling_down_ = false;
    }
    export_slot_range(0, -1);
    import_slot_range(0, -1);
    export_target_str_.clear();
    export_target_.clear();
  }
  name(new_name);
  metadata(status);
  slot_range(new_name);
  update_lock_.unlock();
  RETURN_ERR("!ok");
}

void hash_table_partition::get_storage_size(response &_return, const arg_list &args) {
  if (args.size() != 1) {
    RETURN_ERR("!args_error");
  }
  RETURN_OK(std::to_string(storage_size()), std::to_string(storage_capacity()));
}

void hash_table_partition::get_metadata(response &_return, const arg_list &args) {
  if (args.size() != 1) {
    RETURN_ERR("!args_error");
  }
  RETURN_OK(metadata_);
}

void hash_table_partition::run_command(response &_return, const arg_list &args) {
  auto cmd_name = args[0];
  switch (command_id(cmd_name)) {
    case hash_table_cmd_id::ht_exists:
      exists(_return, args);
      break;
    case hash_table_cmd_id::ht_get:
      get(_return, args);
      break;
    case hash_table_cmd_id::ht_put:
      put(_return, args);
      break;
    case hash_table_cmd_id::ht_upsert:
      upsert(_return, args);
      break;
    case hash_table_cmd_id::ht_remove:
      remove(_return, args);
      break;
    case hash_table_cmd_id::ht_update:
      update(_return, args);
      break;
    case hash_table_cmd_id::ht_update_partition:
      update_partition(_return, args);;
      break;
    case hash_table_cmd_id::ht_get_storage_size:
      get_storage_size(_return, args);
      break;
    case hash_table_cmd_id::ht_get_metadata:
      get_metadata(_return, args);
      break;
    case hash_table_cmd_id::ht_get_range_data:
      get_data_in_slot_range(_return, args);
      break;
    case hash_table_cmd_id::ht_scale_put:
      scale_put(_return, args);
      break;
    case hash_table_cmd_id::ht_scale_remove:
      scale_remove(_return, args);
      break;
    default: {
      _return.emplace_back("!no_such_command");
      return;
    }
  }
  if (is_mutator(cmd_name)) {
    dirty_ = true;
  }
  if (auto_scale_ && is_mutator(cmd_name) && overload() && metadata_ != "exporting" && metadata_ != "importing"
      && is_tail() && !scaling_up_ && !scaling_down_) {
    LOG(log_level::info) << "Overloaded partition; storage = " << storage_size() << " capacity = " << storage_capacity()
                         << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    try {
      scaling_up_ = true;
      std::map<std::string, std::string> scale_conf;
      scale_conf.emplace(std::make_pair(std::string("slot_range_begin"), std::to_string(slot_range_.first)));
      scale_conf.emplace(std::make_pair(std::string("slot_range_end"), std::to_string(slot_range_.second)));
      scale_conf.emplace(std::make_pair(std::string("type"), std::string("hash_table_split")));
      auto scale = std::make_shared<auto_scaling::auto_scaling_client>(auto_scaling_host_, auto_scaling_port_);
      scale->auto_scaling(chain(), path(), scale_conf);
    } catch (std::exception &e) {
      scaling_up_ = false;
      LOG(log_level::warn) << "Split slot range failed: " << e.what();
    }
  }
  if (auto_scale_ && cmd_name == "remove" && underload() && metadata_ != "exporting" && metadata_ != "importing"
      && name() != "0_65536" && is_tail() && !scaling_down_ && !scaling_up_) {
    LOG(log_level::info) << "Underloaded partition; storage = " << storage_size() << " capacity = "
                         << storage_capacity() << " slot range = (" << slot_begin() << ", " << slot_end() << ")";
    try {
      scaling_down_ = true;
      std::map<std::string, std::string> scale_conf;
      scale_conf.emplace(std::make_pair(std::string("type"), std::string("hash_table_merge")));
      scale_conf.emplace(std::make_pair(std::string("storage_capacity"), std::to_string(storage_capacity())));
      auto scale = std::make_shared<auto_scaling::auto_scaling_client>(auto_scaling_host_, auto_scaling_port_);
      scale->auto_scaling(chain(), path(), scale_conf);
    } catch (std::exception &e) {
      scaling_down_ = false;
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
  return dirty_;
}

void hash_table_partition::load(const std::string &path) {
  locked_hash_table_type ltable = block_.lock_table();
  auto remote = persistent::persistent_store::instance(path, ser_);
  auto decomposed = persistent::persistent_store::decompose_path(path);
  remote->read<locked_hash_table_type>(decomposed.second, ltable);
  ltable.unlock();
}

bool hash_table_partition::sync(const std::string &path) {
  if (dirty_) {
    locked_hash_table_type ltable = block_.lock_table();
    auto remote = persistent::persistent_store::instance(path, ser_);
    auto decomposed = persistent::persistent_store::decompose_path(path);
    remote->write<locked_hash_table_type>(ltable, decomposed.second);
    ltable.unlock();
    dirty_ = false;
    return true;
  }
  return false;
}

bool hash_table_partition::dump(const std::string &path) {
  bool flushed = false;
  if (dirty_) {
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
  sub_map_.clear();
  slot_range_.first = 0;
  slot_range_.second = -1;
  state_ = hash_partition_state::regular;
  chain_ = {};
  role_ = singleton;
  scaling_up_ = false;
  scaling_down_ = false;
  dirty_ = false;
  return flushed;
}

void hash_table_partition::forward_all() {
  locked_hash_table_type ltable = block_.lock_table();
  int64_t i = 0;
  for (const auto &entry: ltable) {
    std::vector<std::string> result;
    run_command_on_next(result, {"put", to_string(entry.first), to_string(entry.second)});
    ++i;
  }
  ltable.unlock();
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
