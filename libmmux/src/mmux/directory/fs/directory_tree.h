#ifndef MMUX_DIRECTORY_SERVICE_SHARD_H
#define MMUX_DIRECTORY_SERVICE_SHARD_H

#include <utility>
#include <memory>
#include <atomic>
#include <map>
#include <shared_mutex>
#include <future>

#include "../directory_ops.h"
#include "../block/block_allocator.h"
#include "../../utils/time_utils.h"
#include "../../storage/storage_management_ops.h"
#include "../../utils/directory_utils.h"
#include "../../storage/chain_module.h"
#include "../../utils/logger.h"

namespace mmux {
namespace directory {

class lease_expiry_worker;
class file_size_tracker;
class sync_worker;

class ds_node {
 public:
  explicit ds_node(std::string name, file_status status)
      : name_(std::move(name)), status_(status) {}

  virtual ~ds_node() = default;

  const std::string &name() const { return name_; }

  void name(const std::string &name) { name_ = name; }

  bool is_directory() const { return status_.type() == file_type::directory; }

  bool is_regular_file() const { return status_.type() == file_type::regular; }

  file_status status() const { return status_; }

  directory_entry entry() const { return directory_entry(name_, status_); }

  std::uint64_t last_write_time() const { return status_.last_write_time(); }

  void permissions(const perms &prms) { status_.permissions(prms); }

  perms permissions() const { return status_.permissions(); }

  void last_write_time(std::uint64_t time) { status_.last_write_time(time); }

  virtual void sync(const std::string &backing_path,
                    const std::shared_ptr<storage::storage_management_ops> &storage) = 0;
  virtual void dump(std::vector<std::string> &cleared_blocks,
                    const std::string &backing_path,
                    const std::shared_ptr<storage::storage_management_ops> &storage) = 0;
  virtual void load(const std::string &path,
                    const std::string &backing_path,
                    const std::shared_ptr<storage::storage_management_ops> &storage,
                    const std::shared_ptr<block_allocator> &allocator) = 0;

 private:
  std::string name_{};
  file_status status_{};
};

class ds_file_node : public ds_node {
 public:
  struct export_ctx {
    replica_chain from_block;
    replica_chain to_block;
  };

  explicit ds_file_node(const std::string &name)
      : ds_node(name, file_status(file_type::regular, perms(perms::all), utils::time_utils::now_ms())),
        dstatus_{} {}

  ds_file_node(const std::string &name,
               const std::string &backing_path,
               std::size_t chain_length,
               std::vector<replica_chain> blocks) :
      ds_node(name, file_status(file_type::regular, perms(perms::all), utils::time_utils::now_ms())),
      dstatus_(backing_path, chain_length, std::move(blocks)) {}

  const data_status &dstatus() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_;
  }

  void dstatus(const data_status &status) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_ = status;
  }

  std::vector<storage_mode> mode() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.mode();
  }

  void mode(size_t i, const storage_mode &m) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.mode(i, m);
  }

  void mode(const storage_mode &m) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.mode(m);
  }

  const std::string &backing_path() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.backing_path();
  }

  void backing_path(const std::string &prefix) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.backing_path(prefix);
  }

  std::size_t chain_length() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.chain_length();
  }

  void chain_length(std::size_t chain_length) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.chain_length(chain_length);
  }

  std::int32_t flags() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.flags();
  }

  void flags(std::int32_t flags) {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    dstatus_.flags(flags);
  }

  bool is_pinned() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.is_pinned();
  }

  bool is_mapped() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.is_mapped();
  }

  bool is_static_provisioned() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.is_static_provisioned();
  }

  const std::vector<replica_chain> &data_blocks() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.data_blocks();
  }

  std::vector<replica_chain> _all_data_blocks() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    std::vector<replica_chain> out = dstatus_.data_blocks();
    out.insert(out.end(), adding_.begin(), adding_.end());
    return out;
  }

  void sync(const std::string &backing_path, const std::shared_ptr<storage::storage_management_ops> &storage) override {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    for (const auto &block: dstatus_.data_blocks()) {
      std::string block_backing_path = backing_path;
      utils::directory_utils::push_path_element(block_backing_path, block.slot_range_string());
      if (block.mode == storage_mode::in_memory || block.mode == storage_mode::in_memory_grace)
        storage->sync(block.tail(), block_backing_path);
    }
  }

  void dump(std::vector<std::string> &cleared_blocks,
            const std::string &backing_path,
            const std::shared_ptr<storage::storage_management_ops> &storage) override {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    for (const auto &block: dstatus_.data_blocks()) {
      for (size_t i = 0; i < dstatus_.chain_length(); i++) {
        if (i == dstatus_.chain_length() - 1) {
          std::string block_backing_path = backing_path;
          utils::directory_utils::push_path_element(block_backing_path, block.slot_range_string());
          storage->dump(block.tail(), block_backing_path);
          dstatus_.mark_dumped(i);
        } else {
          storage->reset(block.block_names[i]);
        }
        cleared_blocks.push_back(block.block_names[i]);
      }
    }
  }

  void load(const std::string &path,
            const std::string &backing_path,
            const std::shared_ptr<storage::storage_management_ops> &storage,
            const std::shared_ptr<block_allocator> &allocator) override {
    std::unique_lock<std::shared_mutex> lock(mtx_);

    auto auto_scale = !dstatus_.is_static_provisioned();
    auto num_blocks = dstatus_.data_blocks().size();
    auto chain_length = dstatus_.chain_length();
    std::size_t slots_per_block = storage::block::SLOT_MAX / num_blocks;
    for (std::size_t i = 0; i < num_blocks; ++i) {
      auto slot_begin = static_cast<int32_t>(i * slots_per_block);
      auto slot_end =
          i == (num_blocks - 1) ? storage::block::SLOT_MAX : static_cast<int32_t>((i + 1) * slots_per_block - 1);
      replica_chain chain(allocator->allocate(chain_length, {}),
                          slot_begin,
                          slot_end,
                          chain_status::stable,
                          storage_mode::in_memory);
      assert(chain.block_names.size() == chain_length);
      using namespace storage;
      if (chain_length == 1) {
        std::string block_backing_path = backing_path;
        utils::directory_utils::push_path_element(block_backing_path, chain.slot_range_string());
        storage->setup_block(chain.block_names[0],
                             path,
                             slot_begin,
                             slot_end,
                             chain.block_names,
                             auto_scale,
                             chain_role::singleton,
                             "nil");
        storage->load(chain.block_names[0], block_backing_path);
        dstatus_.mark_loaded(i, chain.block_names);
      } else {
        std::string block_backing_path = backing_path;
        utils::directory_utils::push_path_element(block_backing_path, chain.slot_range_string());
        for (std::size_t j = 0; j < chain_length; ++j) {
          std::string block_name = chain.block_names[j];
          std::string next_block_name = (j == chain_length - 1) ? "nil" : chain.block_names[j + 1];
          int32_t role = (j == 0) ? chain_role::head : (j == chain_length - 1) ? chain_role::tail : chain_role::mid;
          storage->setup_block(block_name,
                               path,
                               slot_begin,
                               slot_end,
                               chain.block_names,
                               auto_scale,
                               role,
                               next_block_name);
          storage->load(block_name, block_backing_path);
        }
        dstatus_.mark_loaded(i, chain.block_names);
      }
    }
  }

  bool handle_lease_expiry(std::vector<std::string> &cleared_blocks,
                           std::shared_ptr<storage::storage_management_ops> storage) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    if (!dstatus_.is_pinned()) {
      using namespace utils;
      LOG(log_level::info) << "Clearing storage for " << name();
      if (dstatus_.is_mapped()) {
        for (const auto &block: dstatus_.data_blocks()) {
          for (size_t i = 0; i < dstatus_.chain_length(); i++) {
            if (i == dstatus_.chain_length() - 1) {
              std::string block_backing_path = dstatus_.backing_path();
              utils::directory_utils::push_path_element(block_backing_path, block.slot_range_string());
              storage->dump(block.tail(), block_backing_path);
              dstatus_.mode(i, storage_mode::on_disk);
            } else {
              storage->reset(block.block_names[i]);
            }
            cleared_blocks.push_back(block.block_names[i]);
          }
        }
        return false; // Clear the blocks, but don't delete the path
      } else {
        for (const auto &block: dstatus_.data_blocks()) {
          for (const auto &block_name: block.block_names) {
            storage->reset(block_name);
            cleared_blocks.push_back(block_name);
          }
        }
      }
      return true; // Clear the blocks and delete the path
    }
    return false; // Don't clear the blocks or delete the path
  }

  export_ctx setup_add_block(std::shared_ptr<storage::storage_management_ops> storage,
                             const std::shared_ptr<block_allocator> &allocator,
                             const std::string &path) {
    using namespace storage;
    if (dstatus_.data_blocks().size() == block::SLOT_MAX)
      throw directory_ops_exception("Cannot expand capacity beyond " + std::to_string(block::SLOT_MAX) + " blocks");

    std::unique_lock<std::shared_mutex> lock(mtx_);
    // Get the block with largest size
    std::vector<std::future<std::size_t>> futures;
    for (const auto &block: dstatus_.data_blocks()) {
      futures.push_back(std::async([&]() -> std::size_t { return storage->storage_size(block.block_names.back()); }));
    }
    size_t i = 0;
    size_t max_size = 0;
    size_t max_pos = 0;
    for (auto &future: futures) {
      size_t sz = future.get();
      auto cstatus = dstatus_.get_data_block_status(i);
      if (sz > max_size && cstatus != chain_status::exporting && cstatus != chain_status::importing
          && dstatus_.num_slots(i) != 1) {
        max_size = sz;
        max_pos = i;
      }
      i++;
    }
    dstatus_.set_data_block_status(max_pos, chain_status::exporting);

    // Split the block's slot range in two
    auto from_chain = dstatus_.data_blocks().at(max_pos);
    auto slot_begin = from_chain.slot_range.first;
    auto slot_end = from_chain.slot_range.second;
    auto slot_mid = (slot_end + slot_begin) / 2; // TODO: We can get a better split...

    // Allocate the new chain
    replica_chain to_chain(allocator->allocate(dstatus_.chain_length(), {}),
                           slot_mid + 1,
                           slot_end,
                           chain_status::stable,
                           storage_mode::in_memory);
    assert(to_chain.block_names.size() == chain_length);

    // Set old chain to exporting and new chain to importing
    if (dstatus_.chain_length() == 1) {
      storage->setup_and_set_importing(to_chain.block_names[0],
                                       path,
                                       slot_mid + 1,
                                       slot_end,
                                       to_chain.block_names,
                                       chain_role::singleton,
                                       "nil");
      storage->set_exporting(from_chain.block_names[0], to_chain.block_names, slot_mid + 1, slot_end);
    } else {
      for (std::size_t j = 0; j < dstatus_.chain_length(); ++j) {
        std::string block_name = to_chain.block_names[j];
        std::string next_block_name = (j == dstatus_.chain_length() - 1) ? "nil" : to_chain.block_names[j + 1];
        int32_t
            role =
            (j == 0) ? chain_role::head : (j == dstatus_.chain_length() - 1) ? chain_role::tail : chain_role::mid;
        storage->setup_and_set_importing(block_name,
                                         path,
                                         slot_mid + 1,
                                         slot_end,
                                         to_chain.block_names,
                                         role,
                                         next_block_name);
      }
      for (std::size_t j = 0; j < dstatus_.chain_length(); ++j) {
        storage->set_exporting(from_chain.block_names[j], to_chain.block_names, slot_mid + 1, slot_end);
      }
    }

    adding_.push_back(to_chain);
    return export_ctx{from_chain, to_chain};
  }

  export_ctx setup_slot_range_split(std::shared_ptr<storage::storage_management_ops> storage,
                                    const std::shared_ptr<block_allocator> &allocator,
                                    const std::string &path,
                                    int32_t slot_begin,
                                    int32_t slot_end) {
    using namespace storage;
    if (dstatus_.data_blocks().size() == block::SLOT_MAX)
      throw directory_ops_exception("Cannot expand capacity beyond " + std::to_string(block::SLOT_MAX) + " blocks");

    std::unique_lock<std::shared_mutex> lock(mtx_);
    size_t block_idx = 0;
    for (const auto &block: dstatus_.data_blocks()) {
      if (block.slot_begin() == slot_begin && block.slot_end() == slot_end) {
        break;
      }
      block_idx++;
    }
    if (block_idx == dstatus_.data_blocks().size()) {
      throw directory_ops_exception(
          "No block with slot range " + std::to_string(slot_begin) + "-" + std::to_string(slot_end));
    }
    auto cstatus = dstatus_.get_data_block_status(block_idx);
    if (cstatus == chain_status::exporting || cstatus == chain_status::importing) {
      throw directory_ops_exception("Block already involved in re-partitioning");
    }
    dstatus_.set_data_block_status(block_idx, chain_status::exporting);

    // Split the block's slot range in two
    auto from_chain = dstatus_.data_blocks().at(block_idx);
    auto slot_mid = (slot_end + slot_begin) / 2; // TODO: We can get a better split...

    // Allocate the new chain
    replica_chain to_chain(allocator->allocate(dstatus_.chain_length(), {}),
                           slot_mid + 1,
                           slot_end,
                           chain_status::stable,
                           storage_mode::in_memory);
    assert(to_chain.block_names.size() == chain_length);

    // Set old chain to exporting and new chain to importing
    if (dstatus_.chain_length() == 1) {
      storage->setup_and_set_importing(to_chain.block_names[0],
                                       path,
                                       slot_mid + 1,
                                       slot_end,
                                       to_chain.block_names,
                                       chain_role::singleton,
                                       "nil");
      storage->set_exporting(from_chain.block_names[0], to_chain.block_names, slot_mid + 1, slot_end);
    } else {
      for (std::size_t j = 0; j < dstatus_.chain_length(); ++j) {
        std::string block_name = to_chain.block_names[j];
        std::string next_block_name = (j == dstatus_.chain_length() - 1) ? "nil" : to_chain.block_names[j + 1];
        int32_t
            role =
            (j == 0) ? chain_role::head : (j == dstatus_.chain_length() - 1) ? chain_role::tail : chain_role::mid;
        storage->setup_and_set_importing(block_name,
                                         path,
                                         slot_mid + 1,
                                         slot_end,
                                         to_chain.block_names,
                                         role,
                                         next_block_name);
      }
      for (std::size_t j = 0; j < dstatus_.chain_length(); ++j) {
        storage->set_exporting(from_chain.block_names[j], to_chain.block_names, slot_mid + 1, slot_end);
      }
    }

    adding_.push_back(to_chain);
    return export_ctx{from_chain, to_chain};
  }

  void finalize_slot_range_split(std::shared_ptr<storage::storage_management_ops> storage, const export_ctx &ctx) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    auto slot_begin = ctx.from_block.slot_begin();
    auto slot_end = ctx.from_block.slot_end();
    auto slot_mid = (slot_end + slot_begin) / 2;

    auto from_idx = dstatus_.find_replica_chain(ctx.from_block);
    dstatus_.update_data_block_slots(from_idx, slot_begin, slot_mid);
    dstatus_.set_data_block_status(from_idx, chain_status::stable);
    dstatus_.add_data_block(ctx.to_block, from_idx + 1);
    auto it = std::find(adding_.begin(), adding_.end(), ctx.to_block);
    if (it == adding_.end()) {
      throw std::logic_error("Cannot find to_block in adding list");
    }
    adding_.erase(it);
    for (std::size_t j = 0; j < dstatus_.chain_length(); ++j) {
      storage->set_regular(ctx.from_block.block_names[j], slot_begin, slot_mid);
      storage->set_regular(ctx.to_block.block_names[j], slot_mid + 1, slot_end);
    }
    using namespace utils;
    LOG(log_level::info) << "Updated file data_status: " << dstatus_.to_string();
  }

  export_ctx setup_slot_range_merge(std::shared_ptr<storage::storage_management_ops> storage,
                                    int32_t slot_begin,
                                    int32_t slot_end) {
    using namespace storage;
    if (dstatus_.data_blocks().size() == 1 || slot_end == block::SLOT_MAX) {
      throw directory_ops_exception("Cannot find a merge partner");
    }
    std::unique_lock<std::shared_mutex> lock(mtx_);
    size_t block_idx = 0;
    for (const auto &block: dstatus_.data_blocks()) {
      if (block.slot_begin() == slot_begin && block.slot_end() == slot_end) {
        break;
      }
      block_idx++;
    }
    if (block_idx == dstatus_.data_blocks().size()) {
      throw directory_ops_exception(
          "No block with slot range " + std::to_string(slot_begin) + "-" + std::to_string(slot_end));
    }
    auto cstatus = dstatus_.get_data_block_status(block_idx);
    if (cstatus == chain_status::exporting || cstatus == chain_status::importing) {
      throw directory_ops_exception("Block already involved in re-partitioning");
    }

    auto from_chain = dstatus_.data_blocks().at(block_idx);
    // Always merge with the right neighbor; we can find the better neighbor, but we just don't
    auto to_chain = dstatus_.data_blocks().at(block_idx + 1);
    if (to_chain.status == chain_status::exporting) {
      throw directory_ops_exception("Cannot find a merge partner");
    }

    dstatus_.set_data_block_status(block_idx, chain_status::exporting);
    dstatus_.set_data_block_status(block_idx + 1, chain_status::importing);

    // Set old chain to exporting and new chain to importing
    if (dstatus_.chain_length() == 1) {
      storage->set_importing(to_chain.block_names[0], slot_begin, slot_end);
      storage->set_exporting(from_chain.block_names[0], to_chain.block_names, slot_begin, slot_end);
    } else {
      for (std::size_t j = 0; j < dstatus_.chain_length(); ++j) {
        storage->set_importing(to_chain.block_names[j], slot_begin, slot_end);
      }
      for (std::size_t j = 0; j < dstatus_.chain_length(); ++j) {
        storage->set_exporting(from_chain.block_names[j], to_chain.block_names, slot_begin, slot_end);
      }
    }
    return export_ctx{from_chain, to_chain};
  }

  void finalize_slot_range_merge(std::shared_ptr<storage::storage_management_ops> storage,
                                 const std::shared_ptr<block_allocator> &allocator,
                                 const export_ctx &ctx) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    auto slot_begin = ctx.from_block.slot_begin();
    auto slot_end = ctx.to_block.slot_end();
    auto to_idx = dstatus_.find_replica_chain(ctx.to_block);
    dstatus_.update_data_block_slots(to_idx, slot_begin, slot_end);
    dstatus_.set_data_block_status(to_idx, chain_status::stable);
    auto from_idx = dstatus_.find_replica_chain(ctx.from_block);
    dstatus_.remove_data_block(from_idx);
    for (std::size_t j = 0; j < dstatus_.chain_length(); ++j) {
      storage->reset(ctx.from_block.block_names[j]);
      storage->set_regular(ctx.to_block.block_names[j], slot_begin, slot_end);
    }
    allocator->free(ctx.from_block.block_names);
    using namespace utils;
    LOG(log_level::info) << "Updated file data_status: " << dstatus_.to_string();
  }

  size_t num_blocks() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.data_blocks().size() + adding_.size();
  }

 private:
  mutable std::shared_mutex mtx_;
  data_status dstatus_{};
  std::vector<replica_chain> adding_{};
};

class ds_dir_node : public ds_node {
 public:
  typedef std::map<std::string, std::shared_ptr<ds_node>> child_map;
  explicit ds_dir_node(const std::string &name)
      : ds_node(name, file_status(file_type::directory, perms(perms::all), utils::time_utils::now_ms())) {}

  std::shared_ptr<ds_node> get_child(const std::string &name) const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    auto ret = children_.find(name);
    if (ret != children_.end()) {
      return ret->second;
    } else {
      return nullptr;
    }
  }

  void add_child(std::shared_ptr<ds_node> node) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    if (children_.find(node->name()) == children_.end()) {
      children_.insert(std::make_pair(node->name(), node));
    } else {
      throw directory_ops_exception("Child node already exists: " + node->name());
    }
  }

  void remove_child(const std::string &name) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    auto ret = children_.find(name);
    if (ret != children_.end()) {
      children_.erase(ret);
    } else {
      throw directory_ops_exception("Child node not found: " + name);
    }
  }

  bool handle_lease_expiry(std::vector<std::string> &cleared_blocks,
                           const std::string &child_name,
                           std::shared_ptr<storage::storage_management_ops> storage) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    auto ret = children_.find(child_name);
    if (ret != children_.end()) {
      if (ret->second->is_regular_file()) {
        auto file = std::dynamic_pointer_cast<ds_file_node>(ret->second);
        if (file->handle_lease_expiry(cleared_blocks, storage)) {
          children_.erase(ret);
          return true;
        }
        return false;
      } else if (ret->second->is_directory()) {
        auto dir = std::dynamic_pointer_cast<ds_dir_node>(ret->second);
        bool cleared = true;
        for (auto &entry: *dir) {
          if (!dir->handle_lease_expiry(cleared_blocks, entry.first, storage)) {
            cleared = false;
          }
        }
        if (cleared)
          children_.erase(ret);
        return cleared;
      }
    } else {
      throw directory_ops_exception("Child node not found: " + child_name);
    }
    return false;
  }

  void sync(const std::string &backing_path, const std::shared_ptr<storage::storage_management_ops> &storage) override {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    for (const auto &entry: children_) {
      entry.second->sync(backing_path, storage);
    }
  }

  void dump(std::vector<std::string> &cleared_blocks,
            const std::string &backing_path,
            const std::shared_ptr<storage::storage_management_ops> &storage) override {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    for (const auto &entry: children_) {
      entry.second->dump(cleared_blocks, backing_path, storage);
    }
  }

  void load(const std::string &path,
            const std::string &backing_path,
            const std::shared_ptr<storage::storage_management_ops> &storage,
            const std::shared_ptr<block_allocator> &allocator) override {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    for (const auto &entry: children_) {
      entry.second->load(path, backing_path, storage, allocator);
    }
  }

  std::vector<directory_entry> entries() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    std::vector<directory_entry> ret;
    ret.reserve(children_.size());
    populate_entries(ret);
    return ret;
  }

  std::vector<directory_entry> recursive_entries() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    std::vector<directory_entry> ret;
    populate_recursive_entries(ret);
    return ret;
  }

  std::vector<std::string> children() const {
    std::vector<std::string> ret;
    for (const auto &entry: children_) {
      ret.push_back(entry.first);
    }
    return ret;
  }

  child_map::const_iterator begin() const {
    return children_.begin();
  }

  child_map::const_iterator end() const {
    return children_.end();
  }

  std::size_t size() const {
    return children_.size();
  }

  bool empty() const {
    return children_.empty();
  }

 private:
  void populate_entries(std::vector<directory_entry> &entries) const {
    for (auto &entry: children_) {
      entries.emplace_back(entry.second->entry());
    }
  }

  void populate_recursive_entries(std::vector<directory_entry> &entries) const {
    for (auto &entry: children_) {
      entries.emplace_back(entry.second->entry());
      if (entry.second->is_directory()) {
        std::dynamic_pointer_cast<ds_dir_node>(entry.second)->populate_recursive_entries(entries);
      }
    }
  }

  mutable std::shared_mutex mtx_;
  child_map children_{};
};

class directory_tree : public directory_ops, public directory_management_ops {
 public:
  explicit directory_tree(std::shared_ptr<block_allocator> allocator,
                          std::shared_ptr<storage::storage_management_ops> storage);

  std::shared_ptr<block_allocator> get_allocator() const {
    return allocator_;
  }

  std::shared_ptr<storage::storage_management_ops> get_storage_manager() const {
    return storage_;
  }

  void create_directory(const std::string &path) override;
  void create_directories(const std::string &path) override;

  data_status open(const std::string &path) override;
  data_status create(const std::string &path,
                     const std::string &backing_path = "",
                     std::size_t num_blocks = 1,
                     std::size_t chain_length = 1,
                     std::int32_t flags = 0) override;
  data_status open_or_create(const std::string &path,
                             const std::string &backing_path = "",
                             std::size_t num_blocks = 1,
                             std::size_t chain_length = 1,
                             std::int32_t flags = 0) override;

  bool exists(const std::string &path) const override;

  std::uint64_t last_write_time(const std::string &path) const override;

  perms permissions(const std::string &path) override;
  void permissions(const std::string &path, const perms &permissions, perm_options opts) override;

  void remove(const std::string &path) override;
  void remove_all(const std::string &path) override;

  void sync(const std::string &path, const std::string &backing_path) override;
  void dump(const std::string &path, const std::string &backing_path) override;
  void load(const std::string &path, const std::string &backing_path) override;

  void rename(const std::string &old_path, const std::string &new_path) override;

  file_status status(const std::string &path) const override;

  std::vector<directory_entry> directory_entries(const std::string &path) override;

  std::vector<directory_entry> recursive_directory_entries(const std::string &path) override;

  data_status dstatus(const std::string &path) override;

  bool is_regular_file(const std::string &path) override;
  bool is_directory(const std::string &path) override;

  void touch(const std::string &path) override;
  replica_chain resolve_failures(const std::string &path,
                                 const replica_chain &chain) override; // TODO: Take id as input
  replica_chain add_replica_to_chain(const std::string &path, const replica_chain &chain) override;
  void add_block_to_file(const std::string &path) override;
  virtual void split_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end);
  virtual void merge_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end);
  virtual void handle_lease_expiry(const std::string &path);

 private:
  std::shared_ptr<ds_node> get_node_unsafe(const std::string &path) const;

  std::shared_ptr<ds_node> get_node(const std::string &path) const;

  std::shared_ptr<ds_dir_node> get_node_as_dir(const std::string &path) const;

  std::shared_ptr<ds_file_node> get_node_as_file(const std::string &path) const;

  std::shared_ptr<ds_node> touch_node_path(const std::string &path, std::uint64_t time) const;

  void clear_storage(std::vector<std::string> &cleared_blocks, std::shared_ptr<ds_node> node);

  void touch(std::shared_ptr<ds_node> node, std::uint64_t time);

  std::shared_ptr<ds_dir_node> root_;
  std::shared_ptr<block_allocator> allocator_;
  std::shared_ptr<storage::storage_management_ops> storage_;

  friend class lease_expiry_worker;
  friend class file_size_tracker;
  friend class sync_worker;
};

}
}

#endif //MMUX_DIRECTORY_SERVICE_SHARD_H
