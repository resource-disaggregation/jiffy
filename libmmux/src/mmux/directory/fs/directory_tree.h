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
/* Directory tree node virtual class */
class ds_node {
 public:

  /**
   * Constructor
   * @param name Node name
   * @param status File status
   */
  explicit ds_node(std::string name, file_status status)
      : name_(std::move(name)), status_(status) {}

  virtual ~ds_node() = default;

  /**
   * @brief Fetch node name
   * @return Node name
   */

  const std::string &name() const { return name_; }

  /**
   * @brief Set node name
   * @param name Name
   */

  void name(const std::string &name) { name_ = name; }

  /**
   * @brief Check if node is directory
   * @return Bool variable, true if node is directory
   */

  bool is_directory() const { return status_.type() == file_type::directory; }

  /**
   * @brief Check if node is regular file
   * @return Bool variable, true if node is regular file
   */

  bool is_regular_file() const { return status_.type() == file_type::regular; }

  /**
   * @brief Fetch file status
   * @return File status
   */
  file_status status() const { return status_; }

  /**
   * @brief Collect entry of Directory
   * @return Directory entry
   */
  directory_entry entry() const { return directory_entry(name_, status_); }

  /**
   * @brief Fetch last write time of file
   * @return Last_write_time
   */

  std::uint64_t last_write_time() const { return status_.last_write_time(); }

  /**
   * @brief Set permissions
   * @param prms Permissions
   */

  void permissions(const perms &prms) { status_.permissions(prms); }

  /**
   * @brief Fetch file permissions
   * @return Permissions
   */

  perms permissions() const { return status_.permissions(); }

  /**
   * @brief Set last write time
   * @param time Last write time
   */

  void last_write_time(std::uint64_t time) { status_.last_write_time(time); }

  /**
   * @brief Virtual function
   * Write all dirty blocks back to persistent storage
   * @param backing_path File backing path
   * @param storage Storage
  */

  virtual void sync(const std::string &backing_path,
                    const std::shared_ptr<storage::storage_management_ops> &storage) = 0;
  /**
   * @brief Virtual function
   * Write all dirty blocks back to persistent storage and clear the block
   * @param cleared_blocks Cleared blocks
   * @param backing_path File backing path
   * @param storage Storage
   */

  virtual void dump(std::vector<std::string> &cleared_blocks,
                    const std::string &backing_path,
                    const std::shared_ptr<storage::storage_management_ops> &storage) = 0;
  /**
   * @brief Virtual function
   * Load blocks from persistent storage
   * @param path File path
   * @param backing_path File backing path
   * @param storage Storage
   * @param allocator Block allocator
   */

  virtual void load(const std::string &path,
                    const std::string &backing_path,
                    const std::shared_ptr<storage::storage_management_ops> &storage,
                    const std::shared_ptr<block_allocator> &allocator) = 0;

 private:
  /* File or directory name */
  std::string name_{};
  /* File or directory status */
  file_status status_{};
};

/**
 * File node class
 * Inherited from virtual class ds_node
 */

class ds_file_node : public ds_node {
 public:
  /**
   * Structure of from chain and to chain
   */
  struct export_ctx {
    replica_chain from_block;
    replica_chain to_block;
  };
  /**
   * @brief Explicit constructor
   * @param name Node name
   */
  explicit ds_file_node(const std::string &name)
      : ds_node(name, file_status(file_type::regular, perms(perms::all), utils::time_utils::now_ms())),
        dstatus_{} {}
  /**
   * @brief Constructor
   * @param name File name
   * @param backing_path File backing_path
   * @param chain_length Chain length
   * @param blocks Number of blocks
   * @param flags Flags
   * @param permissions Permissions
   * @param tags Tags
   */
  ds_file_node(const std::string &name,
               const std::string &backing_path,
               std::size_t chain_length,
               std::vector<replica_chain> blocks,
               int32_t flags,
               int32_t permissions,
               const std::map<std::string, std::string> &tags) :
      ds_node(name,
              file_status(file_type::regular, perms(static_cast<uint16_t>(permissions)), utils::time_utils::now_ms())),
      dstatus_(backing_path, chain_length, std::move(blocks), flags, tags) {}

  /**
   * @brief Fetch data status
   * @return Data status
   */

  const data_status &dstatus() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_;
  }

  /**
   * @brief Set data status
   * @param Data status
   */

  void dstatus(const data_status &status) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_ = status;
  }

  /**
   * @brief Fetch storage mode
   * @return Storage mode
   */

  std::vector<storage_mode> mode() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.mode();
  }

  /**
   * @brief Set new storage mode
   * @param i Block identifier
   * @param m New storage mode
   */

  void mode(size_t i, const storage_mode &m) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.mode(i, m);
  }

  /**
   * @brief Set new storage mode to all data blocks
   * @param m New storage mode
   */
  void mode(const storage_mode &m) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.mode(m);
  }

  /**
   * @brief Fetch backing path of file
   * @return File backing path
   */

  const std::string &backing_path() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.backing_path();
  }

  /**
   * @brief Set prefix backing path
   * @param prefix Prefix backing path
   */

  void backing_path(const std::string &prefix) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.backing_path(prefix);
  }

  /**
   * @brief Fetch chain length
   * @return Chain length
   */

  std::size_t chain_length() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.chain_length();
  }

  /**
   * @brief Set chain length
   * @param chain_length Chain length
   */

  void chain_length(std::size_t chain_length) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.chain_length(chain_length);
  }

  /**
   * @brief Add tag to file
   * @param key Key
   * @param value Value
   */

  void add_tag(const std::string &key, const std::string &value) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.add_tag(key, value);
  }

  /**
   * @brief Add tags to file
   * @param tags Key and value pair
   */

  void add_tags(const std::map<std::string, std::string> &tags) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.add_tags(tags);
  }

  /**
   * @brief Fetch the tag for a given key
   * @param key Key
   * @return tag Tag
   */

  std::string get_tag(const std::string &key) const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.get_tag(key);
  }

  /**
   * @brief Fetch all tags
   * @return Tags
   */

  const std::map<std::string, std::string> &get_tags() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.get_tags();
  }

  /**
   * @brief Fetch all flags
   * @return Flags
   */

  std::int32_t flags() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.flags();
  }

  /**
   * @brief Set flags
   * @param flags Flags
   */

  void flags(std::int32_t flags) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.flags(flags);
  }

  /**
   * @brief Check if data is pinned
   * @return Bool value, true if data is pinned
   */

  bool is_pinned() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.is_pinned();
  }

  /**
   * @brief Check if data is mapped
   * @return Bool value, true if data is mapped
   */

  bool is_mapped() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.is_mapped();
  }

  /**
   * @brief Check if data is static provisioned
   * Check static provisioned bit on flag
   * @return Bool value, true if static provisioned
   */

  bool is_static_provisioned() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.is_static_provisioned();
  }

  /**
   * @brief Fetch data blocks
   * @return Data blocks
   */

  const std::vector<replica_chain> &data_blocks() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.data_blocks();
  }

  /**
   * @brief Fetch all data blocks, including the adding blocks
   * @return Data blocks
   */

  std::vector<replica_chain> _all_data_blocks() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    std::vector<replica_chain> out = dstatus_.data_blocks();
    out.insert(out.end(), adding_.begin(), adding_.end());
    return out;
  }

  /**
   * @brief Write all dirty blocks back to persistent storage
   * @param backing_path File backing path
   * @param storage Storage
   */

  void sync(const std::string &backing_path, const std::shared_ptr<storage::storage_management_ops> &storage) override {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    for (const auto &block: dstatus_.data_blocks()) {
      std::string block_backing_path = backing_path;
      utils::directory_utils::push_path_element(block_backing_path, block.slot_range_string());
      if (block.mode == storage_mode::in_memory || block.mode == storage_mode::in_memory_grace)
        storage->sync(block.tail(), block_backing_path);
    }
  }

  /**
   * @brief Write all dirty blocks back to persistent storage and clear the block
   * @param cleared_blocks Cleared blocks
   * @param backing_path File backing path
   * @param storage Storage
   */

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

  /**
   * @brief Load blocks from persistent storage
   * @param path File path
   * @param backing_path File backing path
   * @param storage Storage
   * @param allocator Block allocator
   */

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

  /**
   * @brief Handle lease expiry
   * Clear storage blocks
   * If it is pinned, do nothing
   * If it is already mapped, then clear the blocks but keep the path
   * Else clear the blocks and also the path
   * @param cleared_blocks Cleared blocks
   * @param storage Storage
   * @return Bool value, true if blocks and path are all deleted
   */

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

  /**
   * @brief Setup old chain and new chain and be ready for splitting
   * Whenever we want to add a new block chain to a file,
   * we find the maximum existing block and set up the new block to split the hash
   * range of the maximum block
   * @param storage Storage
   * @param allocator Block allocator
   * @param path File path
   * @return Export structure of two chains
   */

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

  /**
   * @brief Setup new replica chain to split the slot range block
   * @param storage Storage
   * @param allocator Block allocator
   * @param path File path
   * @param slot_begin Split begin range
   * @param slot_end Split end range
   * @return Export structure of two chains
   */

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
    /* Find block with correct slot range */
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
    /* Make sure that block not already in re-partitioning */
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

  /**
   * @brief Finalize slot range split and update file data status
   * @param storage Storage
   * @param ctx From chain and to chain
   */

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

  /**
   * @brief Setup old chain and new chain and be ready for merging
   * @param storage Storage
   * @param slot_begin Merge begin slot
   * @param slot_end Merge end slot
   * @return Export structure for two chains
   */

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

  /**
   * @brief Finalize slot range merge and update file data status
   * @param storage Storage
   * @param allocator Block allocator
   * @param ctx From chain and to chain
   */

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
  /* Operation mutex */
  mutable std::shared_mutex mtx_;
  /* Data status */
  data_status dstatus_{};
  /* Replica chain that are currently being added to the file(temporary) */
  std::vector<replica_chain> adding_{};
};

/**
 * Directory node class
 * Inherited from general ds_node class
 */
class ds_dir_node : public ds_node {
 public:
  typedef std::map<std::string, std::shared_ptr<ds_node>> child_map;

  /**
   * @brief Explicit constructor
   * @param name Directory name
   */
  explicit ds_dir_node(const std::string &name)
      : ds_node(name, file_status(file_type::directory, perms(perms::all), utils::time_utils::now_ms())) {}

  /**
   * @brief Fetch child node
   * @param name Child name
   * @return Child node
   */

  std::shared_ptr<ds_node> get_child(const std::string &name) const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    auto ret = children_.find(name);
    if (ret != children_.end()) {
      return ret->second;
    } else {
      return nullptr;
    }
  }

  /**
  * @brief Add child node to directory
  * @param node Child node
  */
  void add_child(std::shared_ptr<ds_node> node) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    if (children_.find(node->name()) == children_.end()) {
      children_.insert(std::make_pair(node->name(), node));
    } else {
      throw directory_ops_exception("Child node already exists: " + node->name());
    }
  }

  /**
   * @brief Remove child from directory
   * @param name Child name
   */
  void remove_child(const std::string &name) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    auto ret = children_.find(name);
    if (ret != children_.end()) {
      children_.erase(ret);
    } else {
      throw directory_ops_exception("Child node not found: " + name);
    }
  }

  /**
  * @brief Handle lease expiry recursively for directories
  * @param cleared_blocks Cleared blocks
  * @param child_name Child name
  * @param storage Storage
  * @return Bool value
  */

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

  /**
   * @brief Write all dirty blocks back to persistent storage
   * @param backing_path Backing path
   * @param storage Storage
   */

  void sync(const std::string &backing_path, const std::shared_ptr<storage::storage_management_ops> &storage) override {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    for (const auto &entry: children_) {
      entry.second->sync(backing_path, storage);
    }
  }

  /**
   * @brief Write all dirty blocks back to persistent storage and clear the block
   * @param cleared_blocks Cleared blocks
   * @param backing_path Backing path
   * @param storage Storage
   */

  void dump(std::vector<std::string> &cleared_blocks,
            const std::string &backing_path,
            const std::shared_ptr<storage::storage_management_ops> &storage) override {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    for (const auto &entry: children_) {
      entry.second->dump(cleared_blocks, backing_path, storage);
    }
  }

  /**
   * @brief Load blocks from persistent storage
   * @param path Directory path
   * @param backing_path Backing path
   * @param storage Storage
   * @param allocator Block allocator
   */

  void load(const std::string &path,
            const std::string &backing_path,
            const std::shared_ptr<storage::storage_management_ops> &storage,
            const std::shared_ptr<block_allocator> &allocator) override {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    for (const auto &entry: children_) {
      entry.second->load(path, backing_path, storage, allocator);
    }
  }

  /**
   * @brief Return all entries in directory
   * @return Directory entries
   */

  std::vector<directory_entry> entries() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    std::vector<directory_entry> ret;
    ret.reserve(children_.size());
    populate_entries(ret);
    return ret;
  }

  /**
   * @brief Return all entries in directory recursively
   * @return Directory entries
   */

  std::vector<directory_entry> recursive_entries() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    std::vector<directory_entry> ret;
    populate_recursive_entries(ret);
    return ret;
  }

  /**
  * @brief Return all children names
  * @return Children names
  */

  std::vector<std::string> children() const {
    std::vector<std::string> ret;
    for (const auto &entry: children_) {
      ret.push_back(entry.first);
    }
    return ret;
  }

  /**
   * @brief Fetch beginning child
   * @return Beginning child
   */

  child_map::const_iterator begin() const {
    return children_.begin();
  }

  /**
   * @brief Fetch ending child
   * @return Ending child
   */

  child_map::const_iterator end() const {
    return children_.end();
  }

  /**
   * @brief Fetch number of children
   * @return Number of children
   */

  std::size_t size() const {
    return children_.size();
  }

  /**
   * @brief Check if directory is empty
   * @return Bool variable, true if directory is empty
   */

  bool empty() const {
    return children_.empty();
  }

 private:
  /**
   * @brief Fetch all entries
   * @param entries Entries
   */
  void populate_entries(std::vector<directory_entry> &entries) const {
    for (auto &entry: children_) {
      entries.emplace_back(entry.second->entry());
    }
  }

  /**
   * @brief Fetch all entries recursively
   * @param entries Entries
   */
  void populate_recursive_entries(std::vector<directory_entry> &entries) const {
    for (auto &entry: children_) {
      entries.emplace_back(entry.second->entry());
      if (entry.second->is_directory()) {
        std::dynamic_pointer_cast<ds_dir_node>(entry.second)->populate_recursive_entries(entries);
      }
    }
  }

  /* Operation mutex */
  mutable std::shared_mutex mtx_;

  /* Children of directory */
  child_map children_{};

};

/* Directory tree class */

class directory_tree : public directory_interface {
 public:
  /**
   * @brief Constructor
   * @param allocator Block allocator
   * @param storage Storage management
   */

  explicit directory_tree(std::shared_ptr<block_allocator> allocator,
                          std::shared_ptr<storage::storage_management_ops> storage);

  /**
   * @brief Fetch block allocator
   * @return Block allocator
   */

  std::shared_ptr<block_allocator> get_allocator() const {
    return allocator_;
  }

  /**
   * @brief Fetch storage manager
   * @return Storage manager
   */

  std::shared_ptr<storage::storage_management_ops> get_storage_manager() const {
    return storage_;
  }

  /**
   * @brief Create directory
   * @param path Directory path
   */

  void create_directory(const std::string &path) override;

  /**
   * @brief Create multiple directories
   * @param path Directory paths
   */

  void create_directories(const std::string &path) override;

  /**
   * @brief Open a file given file path
   * @param path File path
   * @return Data status
   */

  data_status open(const std::string &path) override;

  /**
   * @brief Create file
   * @param path File path
   * @param backing_path File backing path
   * @param num_blocks Number of blocks
   * @param chain_length Replica chain length
   * @param flags Flags
   * @param permissions File permissions
   * @param tags Tags
   * @return Data status
   */

  data_status create(const std::string &path,
                     const std::string &backing_path = "",
                     std::size_t num_blocks = 1,
                     std::size_t chain_length = 1,
                     std::int32_t flags = 0,
                     std::int32_t permissions = perms::all(),
                     const std::map<std::string, std::string> &tags = {}) override;

  /**
   * @brief Open or create a file
   * Open file if file exists
   * If not, create it
   * @param path File path
   * @param backing_path File backing path
   * @param num_blocks Number of blocks
   * @param chain_length Replica chain length
   * @param flags Flags
   * @param permissions File permissions
   * @param tags Tags
   * @return File data status
   */

  data_status open_or_create(const std::string &path,
                             const std::string &backing_path = "",
                             std::size_t num_blocks = 1,
                             std::size_t chain_length = 1,
                             std::int32_t flags = 0,
                             std::int32_t permissions = perms::all(),
                             const std::map<std::string, std::string> &tags = {}) override;

  /**
   * @brief Check if the file exists
   * @param path File path
   * @return Bool value, true if file exists
   */

  bool exists(const std::string &path) const override;

  /**
   * @brief Fetch last write time of file
   * @param path File path
   * @return Last write time
   */

  std::uint64_t last_write_time(const std::string &path) const override;

  /**
   * @brief Fetch file permissions
   * @param path File path
   * @return File permissions
   */

  perms permissions(const std::string &path) override;

  /**
   * @brief Set permissions of a file
   * @param path File path
   * @param prms Permission
   * @param opts Permission options
   */

  void permissions(const std::string &path, const perms &permissions, perm_options opts) override;

  /**
   * @brief Remove file given path
   * @param path File path
   */

  void remove(const std::string &path) override;

  /**
   * @brief Remove all files under given directory
   * @param path Directory path
   */

  void remove_all(const std::string &path) override;

  /**
   * @brief Write all dirty blocks back to persistent storage
   * @param path File path
   * @param backing_path File backing path
   */

  void sync(const std::string &path, const std::string &backing_path) override;

  /**
   * @brief Write all dirty blocks back to persistent storage and clear the block
   * @param path File path
   * @param backing_path File backing path
   */

  void dump(const std::string &path, const std::string &backing_path) override;

  /**
   * @brief Load blocks from persistent storage
   * @param path File path
   * @param backing_path File backing path
   */

  void load(const std::string &path, const std::string &backing_path) override;

  /**
   * @brief Rename a file
   * If new file path is a directory path, then put old path file under that directory.
   * If new file path is a file path, overwrite that file with old path file.
   * If new file path doesn't exist, put old path file in new path
   * @param old_path Original file path
   * @param new_path New file path
   */

  void rename(const std::string &old_path, const std::string &new_path) override;

  /**
   * @brief Fetch file status
   * @param path File path
   * @return File status
   */

  file_status status(const std::string &path) const override;

  /**
   * @brief Collect all entries of files in the directory
   * @param path Directory path
   * @return Directory entries
   */

  std::vector<directory_entry> directory_entries(const std::string &path) override;

  /**
   * @brief Collect all entries of files in the directory recursively
   * @param path Directory path
   * @return Directory recursive entries
   */

  std::vector<directory_entry> recursive_directory_entries(const std::string &path) override;

  /**
   * @brief Collect data status
   * @param path File path
   * @return Data status
   */

  data_status dstatus(const std::string &path) override;

  /**
   * @brief Add tags to the file status
   * @param path File path
   * @param tags Tags
   */

  void add_tags(const std::string &path, const std::map<std::string, std::string> &tags) override;

  /**
   * @brief Check if path is regular file
   * @param path File path
   * @return Bool value
   */

  bool is_regular_file(const std::string &path) override;

  /**
   * @brief Check if path is directory
   * @param path File path
   * @return Bool value
   */

  bool is_directory(const std::string &path) override;

  /**
   * @brief Touch file or directory at given path
   * First touch all nodes along the path, then touch all nodes under the path
   * @param path File or directory path
   */

  void touch(const std::string &path) override;

  /**
   * @brief Resolve failure
   * @param path File path
   * @param chain Replica chain
   * @return Replica chain
   */

  replica_chain resolve_failures(const std::string &path,
                                 const replica_chain &chain) override; // TODO: Take id as input

  /**
   * @brief Add a new replica to the chain of the given path
   * @param path File path
   * @param chain Replica chain
   * @return Replica chain
   */

  replica_chain add_replica_to_chain(const std::string &path, const replica_chain &chain) override;

  /**
   * @brief Add block to file
   * @param path File path
   */

  void add_block_to_file(const std::string &path) override;

  /**
   * @brief Split slot range
   * In order to achieve transparent scaling of application memory capacity,
   * when the used capacity exceeds a fixed high threshold, server request new
   * memory block to the file, and split the overloaded block hash range and
   * assign to the new block
   * @param path File path
   * @param slot_begin Split begin range
   * @param slot_end Split end range
   */

  void split_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end) override;

  /**
   * @brief Merge slot range
   * In order to achieve transparent scaling of application memory capacity,
   * when the used capacity falls below a fixed low threshold, merge the hash
   * range associated with the block with another block.
   * @param path File path
   * @param slot_begin Merge begin range
   * @param slot_end Merge end range
   */

  void merge_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end) override;

  /**
   * @brief Handle lease expiry
   * @param path File path
   */

  void handle_lease_expiry(const std::string &path) override;

 private:
  /**
   * @brief Remove file given parent node and child name
   * @param parent Parent node
   * @param child_name Child name
   */

  void remove_all(std::shared_ptr<ds_dir_node> parent, const std::string &child_name);

  /**
   * @brief Get file or directory node, might be NULL ptr
   * @param path File or directory path
   * @return Node
   */

  std::shared_ptr<ds_node> get_node_unsafe(const std::string &path) const;

  /**
   * @brief Get file or directory node, make exception if NULL pointer
   * @param path File or directory path
   * @return Node
   */

  std::shared_ptr<ds_node> get_node(const std::string &path) const;

  /**
   * @brief Get directory node, make exception if not directory
   * @param path Directory path
   * @return Directory node
   */

  std::shared_ptr<ds_dir_node> get_node_as_dir(const std::string &path) const;

  /**
   * @brief Get file node, make exception if not file
   * @param path File path
   * @return File node
   */

  std::shared_ptr<ds_file_node> get_node_as_file(const std::string &path) const;

  /**
   * @brief Touch all nodes along the file path
   * @param path File path
   * @param time Time
   * @return File node
   */

  std::shared_ptr<ds_node> touch_node_path(const std::string &path, std::uint64_t time) const;

  /**
   * @brief Clear storage
   * @param cleared_blocks All blocks that are cleared
   * @param node File or directory node
   */

  void clear_storage(std::vector<std::string> &cleared_blocks, std::shared_ptr<ds_node> node);

  /**
   * @brief Touch file or directory node
   * If file node, modify last write time directly
   * If directory node, modify last write time recursively
   * @param node File or directory node
   * @param time Time
   */

  void touch(std::shared_ptr<ds_node> node, std::uint64_t time);

  /* Root directory */
  std::shared_ptr<ds_dir_node> root_;
  /* Block allocator */
  std::shared_ptr<block_allocator> allocator_;
  /* Storage management */
  std::shared_ptr<storage::storage_management_ops> storage_;

  friend class lease_expiry_worker;
  friend class file_size_tracker;
  friend class sync_worker;
};

}
}

#endif //MMUX_DIRECTORY_SERVICE_SHARD_H
