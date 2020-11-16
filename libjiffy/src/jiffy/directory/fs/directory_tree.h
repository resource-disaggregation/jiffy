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
#include "jiffy/directory/fs/ds_node.h"
#include "jiffy/directory/fs/ds_file_node.h"
#include "jiffy/directory/fs/ds_dir_node.h"

namespace jiffy {
namespace directory {

class lease_expiry_worker;
class file_size_tracker;
class sync_worker;

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
                     const std::vector<std::string> &partition_names = {"0"},
                     const std::vector<std::string> &partition_metadata = {""},
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
                             const std::vector<std::string> &partition_names = {"0"},
                             const std::vector<std::string> &partition_metadata = {""},
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
                                 const replica_chain &chain) override; // TODO: Take partition_name as input

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

  /**
   * @brief Add a block to file
   * @param path File path
   * @param partition_name Name of the partition at new block
   * @param partition_metadata Metadata of the partition at new block
   * @return Replica chain corresponding to the new block
   */
  replica_chain add_block(const std::string &path,
                          const std::string &partition_name,
                          const std::string &partition_metadata) override;

  /**
   * @brief Remove block from the file
   * @param path File path
   * @param partition_name Name of the partition at the block.
   */
  void remove_block(const std::string &path, const std::string &partition_name) override;

  /**
   * @brief Update partition name and metadata
   * @param path File path
   * @param old_partition_name Old partition name
   * @param new_partition_name New partition name to be set
   * @param partition_metadata New partition metadata to be set
   */
  void update_partition(const std::string &path,
                        const std::string &old_partition_name,
                        const std::string &new_partition_name,
                        const std::string &partition_metadata) override;
  /**
   * @brief Fetch the capacity of the partition
   * @param path File path
   * @param partition_name Partition name
   * @return Storage capacity
   */
  int64_t get_capacity(const std::string &path, const std::string &partition_name) override;

 private:
  /**
   * @brief Remove file given parent node and child name
   * @param parent Parent node
   * @param child_name Child name
   */

  void remove_all(const std::string &path, std::shared_ptr<ds_dir_node> parent, const std::string &child_name);

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
