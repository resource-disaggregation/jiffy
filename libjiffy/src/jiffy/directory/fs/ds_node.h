#ifndef JIFFY_DS_NODE_H
#define JIFFY_DS_NODE_H

#include <string>
#include <vector>
#include "jiffy/directory/block/block_allocator.h"
#include "jiffy/directory/directory_ops.h"
#include "jiffy/storage/storage_management_ops.h"

namespace jiffy {
namespace directory {

/* Directory tree node virtual class */
class ds_node {
 public:

  /**
   * Constructor
   * @param name Node name
   * @param status File status
   */
  explicit ds_node(std::string name, file_status status);

  virtual ~ds_node() = default;

  /**
   * @brief Fetch node name
   * @return Node name
   */
  const std::string &name() const;

  /**
   * @brief Set node name
   * @param name Name
   */
  void name(const std::string &name) { name_ = name; }

  /**
   * @brief Check if node is directory
   * @return Bool variable, true if node is directory
   */
  bool is_directory() const;

  /**
   * @brief Check if node is regular file
   * @return Bool variable, true if node is regular file
   */
  bool is_regular_file() const;

  /**
   * @brief Fetch file status
   * @return File status
   */
  file_status status() const;

  /**
   * @brief Collect entry of Directory
   * @return Directory entry
   */
  directory_entry entry() const;

  /**
   * @brief Fetch last write time of file
   * @return Last_write_time
   */

  std::uint64_t last_write_time() const;

  /**
   * @brief Set permissions
   * @param prms Permissions
   */

  void permissions(const perms &prms);

  /**
   * @brief Fetch file permissions
   * @return Permissions
   */

  perms permissions() const;

  /**
   * @brief Set last write time
   * @param time Last write time
   */

  void last_write_time(std::uint64_t time);

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

}
}

#endif //JIFFY_DS_NODE_H
