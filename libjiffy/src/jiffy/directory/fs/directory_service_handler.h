#ifndef JIFFY_DIRECTORY_RPC_SERVICE_HANDLER_H
#define JIFFY_DIRECTORY_RPC_SERVICE_HANDLER_H

#include "directory_service.h"
#include "directory_tree.h"

namespace jiffy {
namespace directory {

/**
 * Directory service handler class
 * Inherited from directory_serviceIF
 */

class directory_service_handler : public directory_serviceIf {
 public:
  /**
   * @brief Constructor
   * @param shard Directory tree
   */

  explicit directory_service_handler(std::shared_ptr<directory_tree> shard);

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
   * @param _return RPC data status to be collected
   * @param path File path
   */

  void open(rpc_data_status &_return, const std::string &path) override;

  /**
   * @brief Create file
   * @param _return RPC data status to be collected
   * @param path File path
   * @param type Data type
   * @param backing_path File backing path
   * @param num_blocks Number of blocks
   * @param chain_length Replica chain length
   * @param flags Flags
   * @param permissions File permissions
   * @param block_names Block names
   * @param block_metadata Block metadata
   * @param tags Tags
   */

  void create(rpc_data_status &_return, const std::string &path, const std::string &type,
              const std::string &backing_path, int32_t num_blocks, int32_t chain_length, int32_t flags,
              int32_t permissions, const std::vector<std::string> &block_names,
              const std::vector<std::string> &block_metadata, const std::map<std::string, std::string> &tags) override;

  /**
   * @brief Open or create a file
   * Open file if file exists
   * If not, create it
   * @param _return RPC data status to be collected
   * @param path File path
   * @param type Data type
   * @param backing_path File backing path
   * @param num_blocks Number of blocks
   * @param chain_length Replica chain length
   * @param flags Flags
   * @param permissions File permissions
   * @param block_names Block names
   * @param block_metadata Block metadata
   * @param tags Tags
   */

  void open_or_create(rpc_data_status &_return, const std::string &path, const std::string &type,
                      const std::string &backing_path, int32_t num_blocks, int32_t chain_length, int32_t flags,
                      int32_t permissions, const std::vector<std::string> &block_names,
                      const std::vector<std::string> &block_metadata,
                      const std::map<std::string, std::string> &tags) override;

  /**
   * @brief Check if the file exists
   * @param path File path
   * @return Bool value, true if exists
   */

  bool exists(const std::string &path) override;

  /**
   * @brief Fetch last write time of file
   * @param path File path
   * @return Last write time
   */

  int64_t last_write_time(const std::string &path) override;

  /**
   * @brief Set permissions of a file
   * @param path File path
   * @param prms RPC Permission
   * @param opts RPC Permission options
   */

  void set_permissions(const std::string &path, rpc_perms perms, rpc_perm_options opts) override;

  /**
   * @brief Fetch permission of a file
   * @param path File path
   * @return RPC Permission
   */

  rpc_perms get_permissions(const std::string &path) override;

  /**
   * @brief Remove file
   * @param path File path
   */

  void remove(const std::string &path) override;

  /**
   * @brief Remove all files under given directory
   * @param path Directory path
   */

  void remove_all(const std::string &path) override;

  /**
   * @brief Rename a file
   * @param old_path Original file path
   * @param new_path New file path
   */

  void rename(const std::string &old_path, const std::string &new_path) override;

  /**
   * @brief Fetch file status
   * @param _return File status to be collected
   * @param path File path
   */

  void status(rpc_file_status &_return, const std::string &path) override;

  /**
   * @brief Collect all entries of files in the directory
   * @param _return RPC directory entries to be collected
   * @param path Directory path
   */

  void directory_entries(std::vector<rpc_dir_entry> &_return, const std::string &path) override;

  /**
   * @brief Collect all entries of files in the directory recursively
   * @param _return RPC directory entries to be collected
   * @param path Directory path
   */

  void recursive_directory_entries(std::vector<rpc_dir_entry> &_return, const std::string &path) override;

  /**
   * @brief Collect data status
   * @param _return RPC data status to be collected
   * @param path File path
   */

  void dstatus(rpc_data_status &_return, const std::string &path) override;

  /**
   * @brief Add tags to the file status
   * @param path File path
   * @param tags Tags
   */

  void add_tags(const std::string &path, const std::map<std::string, std::string> &tags) override;

  /**
   * @brief Check if path is regular file
   * @param path File path
   * @return Bool value, true if file is regular file
   */

  bool is_regular_file(const std::string &path) override;

  /**
   * @brief Check if path is directory
   * @param path Directory path
   * @return Bool value, true if path is directory
   */

  bool is_directory(const std::string &path) override;

  /**
   * @brief Resolve failures using replica chain
   * @param _return RPC replica chain to be collected
   * @param path File path
   * @param chain RPC replica chain
   */

  void reslove_failures(rpc_replica_chain &_return, const std::string &path, const rpc_replica_chain &chain) override;

  /**
   * @brief Add a new replica to chain
   * @param _return RPC replica chain status to be collected
   * @param path File path
   * @param chain RPC replica chain
   */

  void add_replica_to_chain(rpc_replica_chain &_return,
                            const std::string &path,
                            const rpc_replica_chain &chain) override;

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
   * @brief Add replica chain
   * @param _return Replica chain
   * @param path File path
   * @param partition_name New partition name
   * @param partition_metadata New partition metadata
   */
  void add_data_block(rpc_replica_chain &_return,
                      const std::string &path,
                      const std::string &partition_name,
                      const std::string &partition_metadata) override;

  /**
   * @brief Remove chain
   * @param path File path
   * @param partition_name Partition name to be removed
   */
  void remove_data_block(const std::string &path, const std::string &partition_name) override;

  /**
   * @brief Update partition data
   * @param path File path
   * @param old_partition_name Old partition name
   * @param new_partition_name New partition name
   * @param partition_metadata New partition metadata
   */
  void request_partition_data_update(const std::string &path,
                                     const std::string &old_partition_name,
                                     const std::string &new_partition_name,
                                     const std::string &partition_metadata) override;

  /**
   * @brief Fetch storage capacity
   * @param path File path
   * @param partition_name Partition name
   * @return Storage capacity
   */
  int64_t get_storage_capacity(const std::string &path, const std::string &partition_name) override;

 private:
  /**
   * @brief Make exceptions
   * @param ex Exception
   * @return Exception message
   */

  directory_service_exception make_exception(directory_ops_exception &ex) const;
  /* Directory tree */
  std::shared_ptr<directory_tree> shard_;
};
}
}

#endif //JIFFY_DIRECTORY_RPC_SERVICE_HANDLER_H
