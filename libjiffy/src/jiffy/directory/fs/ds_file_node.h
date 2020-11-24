#ifndef JIFFY_DS_FILE_NODE_H
#define JIFFY_DS_FILE_NODE_H

#include "jiffy/directory/fs/ds_node.h"

namespace jiffy {
namespace directory {

/**
 * File node class
 * Inherited from virtual class ds_node
 */

class ds_file_node : public ds_node {
 public:
  /**
   * @brief Explicit constructor
   * @param name Node name
   */
  explicit ds_file_node(const std::string &name);
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
               const std::map<std::string, std::string> &tags);

  /**
   * @brief Fetch data status
   * @return Data status
   */

  const data_status &dstatus() const;

  /**
   * @brief Set data status
   * @param Data status
   */

  void dstatus(const data_status &status);

  /**
   * @brief Fetch storage mode
   * @return Storage mode
   */

  std::vector<storage_mode> mode() const;

  /**
   * @brief Set new storage mode
   * @param i Block identifier
   * @param m New storage mode
   */

  void mode(size_t i, const storage_mode &m);

  /**
   * @brief Set new storage mode to all data blocks
   * @param m New storage mode
   */
  void mode(const storage_mode &m);

  /**
   * @brief Fetch backing path of file
   * @return File backing path
   */

  const std::string &backing_path() const;

  /**
   * @brief Set prefix backing path
   * @param prefix Prefix backing path
   */

  void backing_path(const std::string &prefix);

  /**
   * @brief Fetch chain length
   * @return Chain length
   */

  std::size_t chain_length() const;

  /**
   * @brief Set chain length
   * @param chain_length Chain length
   */

  void chain_length(std::size_t chain_length);

  /**
   * @brief Add tag to file
   * @param key Key
   * @param value Value
   */

  void add_tag(const std::string &key, const std::string &value);

  /**
   * @brief Add tags to file
   * @param tags Key and value pair
   */

  void add_tags(const std::map<std::string, std::string> &tags);

  /**
   * @brief Fetch the tag for a given key
   * @param key Key
   * @return tag Tag
   */

  std::string get_tag(const std::string &key) const;

  /**
   * @brief Fetch all tags
   * @return Tags
   */

  const std::map<std::string, std::string> &get_tags() const;

  /**
   * @brief Fetch all flags
   * @return Flags
   */

  std::int32_t flags() const;

  /**
   * @brief Set flags
   * @param flags Flags
   */

  void flags(std::int32_t flags);

  /**
   * @brief Check if data is pinned
   * @return Bool value, true if data is pinned
   */

  bool is_pinned() const;

  /**
   * @brief Check if data is mapped
   * @return Bool value, true if data is mapped
   */

  bool is_mapped() const;

  /**
   * @brief Check if data is static provisioned
   * Check static provisioned bit on flag
   * @return Bool value, true if static provisioned
   */

  bool is_static_provisioned() const;

  /**
   * @brief Fetch data blocks
   * @return Data blocks
   */

  const std::vector<replica_chain> &data_blocks() const;

  /**
   * Add data block to file node
   * @param partition_name Name of the partition at new block
   * @param partition_metadata Metadata of the partition at new block
   * @param storage Storage
   * @param allocator Block allocator
   * @return Replica chain corresponding to the new block
   */
  replica_chain add_data_block(const std::string &path,
                               const std::string &partition_name,
                               const std::string &partition_metadata,
                               const std::shared_ptr<storage::storage_management_ops> &storage,
                               const std::shared_ptr<block_allocator> &allocator);

  /**
   * Remove data block from the file node
   * @param partition_name Name of the partition at the block.
   * @param storage Storage
   * @param allocator Block allocator
   */
  void remove_block(const std::string &partition_name,
                    const std::shared_ptr<storage::storage_management_ops> &storage,
                    const std::shared_ptr<block_allocator> &allocator,
                    const std::string &tenant_id);

  /**
   * @brief Write all dirty blocks back to persistent storage
   * @param backing_path File backing path
   * @param storage Storage
   */

  void sync(const std::string &backing_path, const std::shared_ptr<storage::storage_management_ops> &storage) override;

  /**
   * @brief Write all dirty blocks back to persistent storage and clear the block
   * @param cleared_blocks Cleared blocks
   * @param backing_path File backing path
   * @param storage Storage
   */

  void dump(std::vector<std::string> &cleared_blocks,
            const std::string &backing_path,
            const std::shared_ptr<storage::storage_management_ops> &storage) override;

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
            const std::shared_ptr<block_allocator> &allocator) override;

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
                           const std::shared_ptr<storage::storage_management_ops>& storage);

  /**
   * Get the number of blocks in this file node.
   * @return Number of blocks in this file node.
   */
  size_t num_blocks() const;

  /**
   * @brief Update a partition name and metadata
   * @param old_name Old partition name
   * @param new_name New partition name
   * @param metadata Partition metadata
   */
  void update_data_status_partition(const std::string &old_name, const std::string &new_name, const std::string &metadata);

 private:
  /* Operation mutex */
  mutable std::mutex mtx_;
  /* Data status */
  data_status dstatus_{};
};

}
}

#endif //JIFFY_DS_FILE_NODE_H
