#ifndef JIFFY_DIRECTORY_CLIENT_H
#define JIFFY_DIRECTORY_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "../directory_ops.h"
#include "../fs/directory_service.h"

namespace jiffy {
namespace directory {

/* Directory client class, inherited from directory_interface */

class directory_client : public directory_interface {
 public:
  typedef directory_serviceClient thrift_client;

  directory_client() = default;

  /**
   * @brief Destructor
   */

  ~directory_client() override;

  /**
   * @brief Constructor
   * @param hostname Directory server hostname
   * @param port Port number
   */

  directory_client(const std::string &hostname, int port);

  /**
   * @brief Connect server
   * @param hostname Directory server hostname
   * @param port Port number
   */

  void connect(const std::string &hostname, int port);

  /**
   * @brief Disconnect server
   */

  void disconnect();

  /**
   * @brief Create directory
   * @param path Directory path
   */

  void create_directory(const std::string &path) override;

  /**
   * @brief Create directories
   * @param path Directory paths
   */

  void create_directories(const std::string &path) override;

  /**
   * @brief Open a file
   * @param path File path
   * @return Data status
   */

  data_status open(const std::string &path) override;

  /**
   * @brief Create a file
   * @param path File path
   * @param backing_path File backing path
   * @param num_blocks Number of blocks
   * @param chain_length Replica chain length
   * @param flags Flag arguments
   * @param permissions Permissions
   * @param tags Tag arguments
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
   * @brief Open if exist, otherwise create
   * @param path File path
   * @param backing_path Backing_path
   * @param num_blocks Number of blocks
   * @param chain_length Replica chain length
   * @param flags Flag arguments
   * @param permissions Permissions
   * @param tags Tag arguments
   * @return Data status
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
   * @return Bool value, true if exists
   */

  bool exists(const std::string &path) const override;

  /**
   * @brief Fetch last write time of a file
   * @param path File path
   * @return Last write time
   */

  std::uint64_t last_write_time(const std::string &path) const override;

  /**
   * @brief Fetch permissions of a file
   * @param path File path
   * @return Permissions
   */

  perms permissions(const std::string &path) override;

  /**
   * @brief Set permissions
   * @param path File path
   * @param prms Permissions
   * @param opts Permission options
   */

  void permissions(const std::string &path, const perms &prms, perm_options opts) override;

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
   * @brief Write all dirty blocks back to persistent storage
   * @param path File path
   * @param backing_path File backing path
   */

  void sync(const std::string &path, const std::string &backing_path) override;

  /**
   * @brief Rename a file
   * @param old_path Original file path
   * @param new_path New file path
   */

  void rename(const std::string &old_path, const std::string &new_path) override;

  /**
   * @brief Fetch file status
   * @param path file path
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
   * @return Directory entries
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
   * @brief Check if file is regular file
   * @param path File path
   * @return Bool value, true if file is regular file
   */

  bool is_regular_file(const std::string &path) override;

  /**
   * @brief Check if file is directory
   * @param path Directory path
   * @return Bool value, true if file is directory
   */

  bool is_directory(const std::string &path) override;

  // Management Ops
  /**
   * @brief Resolve failures using replica chain
   * @param path File path
   * @param chain Replica chain
   * @return Replica chain
   */

  replica_chain resolve_failures(const std::string &path, const replica_chain &chain) override;

  /**
   * @brief Add a new replica to chain
   * @param path File path
   * @param chain Replica chain
   * @return Replica chain
   */

  replica_chain add_replica_to_chain(const std::string &path, const replica_chain &chain) override;

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
   * @brief Touch -> unsupported operation
   */

  void touch(const std::string &path) override;

  /**
   * @brief Handle lease expiry -> unsupported operation
   */

  void handle_lease_expiry(const std::string &path) override;

  /**
   * @brief Add a new replica chain to the path
   * @param path File path
   * @param partition_name Partition name
   * @param partition_metadata Partition metadata
   * @return The new replica chain structure
   */
  replica_chain add_block(const std::string &path,
                          const std::string &partition_name,
                          const std::string &partition_metadata) override;

  /**
   * @brief Remove a replica chain
   * @param path File path
   * @param partition_name Partition name to be removed
   */
  void remove_block(const std::string &path, const std::string &partition_name) override;

  /**
   * @brief Update partition name and metadata
   * @param path File path
   * @param partition_name New partition name
   * @param partition_metadata New partition metadata
   */
  void update_partition(const std::string &path,
                        const std::string &old_partition_name,
                        const std::string &new_partition_name,
                        const std::string &partition_metadata) override;

  /**
   * @brief Fetch partition capacity
   * @param path File path
   * @param partition_name Partition name
   * @return Partition capacity
   */
  int64_t get_capacity(const std::string &path, const std::string &partition_name) override;

 private:
  /* Socket */
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  /* Transport */
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  /* Protocol */
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  /* Client */
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //JIFFY_DIRECTORY_CLIENT_H
