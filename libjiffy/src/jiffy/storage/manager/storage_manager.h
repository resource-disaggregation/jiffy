#ifndef JIFFY_KV_MANAGER_H
#define JIFFY_KV_MANAGER_H

#include <vector>
#include <string>

#include "../storage_management_ops.h"

namespace jiffy {
namespace storage {
/* Storage manager class
 * Inherited from storage_management_ops virtual class */
class storage_manager : public storage_management_ops {
 public:
  storage_manager() = default;

  virtual ~storage_manager() = default;

  /**
   * @brief Setup block
   * @param block_name Partition name
   * @param path Block path
   * @param chain Chain block names
   * @param role Block role
   * @param next_block_name Next block's name
   */

  void setup_block(const std::string &block_name,
                   const std::string &path,
                   const std::string &partition_type,
                   const std::string &partition_name,
                   const std::string &partition_metadata,
                   const std::vector<std::string> &chain,
                   int32_t role,
                   const std::string &next_block_name) override;

  /**
   * @brief Fetch block path
   * @param block_name Block name
   * @return Block path
   */

  std::string path(const std::string &block_name) override;

  /**
   * @brief Load block from persistent storage
   * @param block_name Block name
   * @param backing_path Block backing path
   */

  void load(const std::string &block_name, const std::string &backing_path) override;

  /**
   * @brief Synchronize block and persistent storage
   * @param block_name Block name
   * @param backing_path Block backing path
   */

  void sync(const std::string &block_name, const std::string &backing_path) override;

  /**
   * @brief Write block back to persistent storage and clear the block
   * @param block_name Block name
   * @param backing_path Block backing path
   */

  void dump(const std::string &block_name, const std::string &backing_path) override;

  /**
   * @brief Reset block
   * @param block_name Block name
   */

  void reset(const std::string &block_name) override;

  /**
   * @brief Fetch storage capacity of block
   * @param block_name Block name
   * @return Storage capacity
   */

  size_t storage_capacity(const std::string &block_name) override;

  /**
   * @brief Fetch storage size of block
   * @param block_name Block name
   * @return Storage size
   */

  size_t storage_size(const std::string &block_name) override;

  /**
   * @brief Resending pending request
   * @param block_name Block name
   */

  void resend_pending(const std::string &block_name) override;

  /**
   * @brief Forward all key and values to next block
   * @param block_name Block name
   */

  void forward_all(const std::string &block_name) override;
};

}
}

#endif //JIFFY_KV_MANAGER_H
