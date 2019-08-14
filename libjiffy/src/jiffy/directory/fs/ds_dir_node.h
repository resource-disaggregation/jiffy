#ifndef JIFFY_DS_DIR_NODE_H
#define JIFFY_DS_DIR_NODE_H

#include "jiffy/directory/fs/ds_node.h"

namespace jiffy {
namespace directory {

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
  explicit ds_dir_node(const std::string &name);

  /**
   * @brief Fetch child node
   * @param name Child name
   * @return Child node
   */

  std::shared_ptr<ds_node> get_child(const std::string &name) const;

  /**
  * @brief Add child node to directory
  * @param node Child node
  */
  void add_child(std::shared_ptr<ds_node> node);

  /**
   * @brief Remove child from directory
   * @param name Child name
   */
  void remove_child(const std::string &name);

  /**
  * @brief Handle lease expiry recursively for directories
  * @param cleared_blocks Cleared blocks
  * @param child_name Child name
  * @param storage Storage
  * @return Bool value
  */

  bool handle_lease_expiry(std::vector<std::string> &cleared_blocks,
                           const std::string &child_name,
                           std::shared_ptr<storage::storage_management_ops> storage);

  /**
   * @brief Write all dirty blocks back to persistent storage
   * @param backing_path Backing path
   * @param storage Storage
   */

  void sync(const std::string &backing_path,
            const std::shared_ptr<storage::storage_management_ops> &storage) override;

  /**
   * @brief Write all dirty blocks back to persistent storage and clear the block
   * @param cleared_blocks Cleared blocks
   * @param backing_path Backing path
   * @param storage Storage
   */

  void dump(std::vector<std::string> &cleared_blocks,
            const std::string &backing_path,
            const std::shared_ptr<storage::storage_management_ops> &storage) override;

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
            const std::shared_ptr<block_allocator> &allocator) override;

  /**
   * @brief Return all entries in directory
   * @return Directory entries
   */

  std::vector<directory_entry> entries() const;

  /**
   * @brief Return all entries in directory recursively
   * @return Directory entries
   */

  std::vector<directory_entry> recursive_entries() const;

  /**
  * @brief Return all child names
  * @return Child names
  */

  std::vector<std::string> child_names() const;

  /**
   * @brief Return the child map
   * @return The child map
   */
  child_map children() const;

  /**
   * @brief Fetch beginning child
   * @return Beginning child
   */

  child_map::const_iterator begin() const { return children_.begin(); }

  /**
   * @brief Fetch ending child
   * @return Ending child
   */

  child_map::const_iterator end() const { return children_.end(); }

  /**
   * @brief Fetch number of children
   * @return Number of children
   */

  std::size_t size() const { return children_.size(); }

  /**
   * @brief Check if directory is empty
   * @return Bool variable, true if directory is empty
   */

  bool empty() const { return children_.empty(); }

 private:
  /**
   * @brief Fetch all entries
   * @param entries Entries
   */
  void populate_entries(std::vector<directory_entry> &entries) const;

  /**
   * @brief Fetch all entries recursively
   * @param entries Entries
   */
  void populate_recursive_entries(std::vector<directory_entry> &entries) const;

  /* Operation mutex */
  mutable std::mutex mtx_;

  /* Children of directory */
  child_map children_{};

};

}
}

#endif //JIFFY_DS_DIR_NODE_H
