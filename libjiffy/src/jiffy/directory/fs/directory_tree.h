#ifndef JIFFY_DIRECTORY_SERVICE_SHARD_H
#define JIFFY_DIRECTORY_SERVICE_SHARD_H

#include <utility>
#include <memory>
#include <atomic>
#include <map>
#include <shared_mutex>
#include <future>

#include "jiffy/directory/directory_ops.h"
#include "jiffy/directory/block/block_allocator.h"
#include "jiffy/utils/time_utils.h"
#include "jiffy/storage/storage_management_ops.h"
#include "jiffy/utils/directory_utils.h"
#include "jiffy/storage/chain_module.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
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
   * @param type File type
   * @param backing_path File backing_path
   * @param chain_length Chain length
   * @param blocks Number of blocks
   * @param flags Flags
   * @param permissions Permissions
   * @param tags Tags
   */
  ds_file_node(const std::string &name,
               const std::string &type,
               const std::string &backing_path,
               std::size_t chain_length,
               std::vector<replica_chain> blocks,
               int32_t flags,
               int32_t permissions,
               const std::map<std::string, std::string> &tags) :
      ds_node(name,
              file_status(file_type::regular, perms(static_cast<uint16_t>(permissions)), utils::time_utils::now_ms())),
      dstatus_(type, backing_path, chain_length, std::move(blocks), flags, tags) {}

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
   * @brief Write all dirty blocks back to persistent storage
   * @param backing_path File backing path
   * @param storage Storage
   */

  void sync(const std::string &backing_path, const std::shared_ptr<storage::storage_management_ops> &storage) override {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    for (const auto &block: dstatus_.data_blocks()) {
      std::string block_backing_path = backing_path;
      utils::directory_utils::push_path_element(block_backing_path, block.name);
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
          utils::directory_utils::push_path_element(block_backing_path, block.name);
          storage->dump(block.tail(), block_backing_path);
          dstatus_.mark_dumped(i);
        } else {
          storage->destroy_partition(block.block_names[i]);
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

    auto num_blocks = dstatus_.data_blocks().size();
    auto chain_length = dstatus_.chain_length();
    for (std::size_t i = 0; i < num_blocks; ++i) {
      replica_chain chain(allocator->allocate(chain_length, {}), storage_mode::in_memory);
      assert(chain.block_names.size() == chain_length);
      using namespace storage;
      if (chain_length == 1) {
        std::string block_backing_path = backing_path;
        utils::directory_utils::push_path_element(block_backing_path, chain.name);
        storage->create_partition(chain.block_names[0], dstatus_.type(), chain.name, chain.metadata, get_tags());
        storage->setup_chain(chain.block_names[0], path, chain.block_names, chain_role::singleton, "nil");
        storage->load(chain.block_names[0], block_backing_path);
        dstatus_.mark_loaded(i, chain.block_names);
      } else {
        std::string block_backing_path = backing_path;
        utils::directory_utils::push_path_element(block_backing_path, chain.name);
        for (std::size_t j = 0; j < chain_length; ++j) {
          std::string block_name = chain.block_names[j];
          std::string next_block_name = (j == chain_length - 1) ? "nil" : chain.block_names[j + 1];
          int32_t role = (j == 0) ? chain_role::head : (j == chain_length - 1) ? chain_role::tail : chain_role::mid;
          storage->create_partition(block_name, dstatus_.type(), chain.name, chain.metadata, get_tags());
          storage->setup_chain(block_name, path, chain.block_names, role, next_block_name);
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
              utils::directory_utils::push_path_element(block_backing_path, block.name);
              storage->dump(block.tail(), block_backing_path);
              dstatus_.mode(i, storage_mode::on_disk);
            } else {
              storage->destroy_partition(block.block_names[i]);
            }
            cleared_blocks.push_back(block.block_names[i]);
          }
        }
        return false; // Clear the blocks, but don't delete the path
      } else {
        for (const auto &block: dstatus_.data_blocks()) {
          for (const auto &block_name: block.block_names) {
            storage->destroy_partition(block_name);
            cleared_blocks.push_back(block_name);
          }
        }
      }
      return true; // Clear the blocks and delete the path
    }
    return false; // Don't clear the blocks or delete the path
  }

  size_t num_blocks() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.data_blocks().size();
  }

 private:
  /* Operation mutex */
  mutable std::shared_mutex mtx_;
  /* Data status */
  data_status dstatus_{};
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
        auto entries = dir->children();
        for (auto &entry: entries) {
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
  * @brief Return all child names
  * @return Child names
  */

  std::vector<std::string> child_names() const {
    std::vector<std::string> ret;
    for (const auto &entry: children_) {
      ret.push_back(entry.first);
    }
    return ret;
  }

  /**
   * @brief Return the child map
   * @return The child map
   */
  child_map children() const {
    return children_;
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
                     const std::string &type,
                     const std::string &backing_path = "",
                     int32_t num_blocks = 1,
                     int32_t chain_length = 1,
                     int32_t flags = 0,
                     int32_t permissions = perms::all(),
                     const std::vector<std::string> &block_names = {"0"},
                     const std::vector<std::string> &block_metadata = {""},
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
                             const std::string &type,
                             const std::string &backing_path = "",
                             int32_t num_blocks = 1,
                             int32_t chain_length = 1,
                             int32_t flags = 0,
                             int32_t permissions = perms::all(),
                             const std::vector<std::string> &block_names = {"0"},
                             const std::vector<std::string> &block_metadata = {""},
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

#endif //JIFFY_DIRECTORY_SERVICE_SHARD_H
