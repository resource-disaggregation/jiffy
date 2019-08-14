#ifndef JIFFY_KV_MANAGER_H
#define JIFFY_KV_MANAGER_H

#include <vector>
#include <string>
#include <jiffy/utils/property_map.h>

#include "../storage_management_ops.h"

namespace jiffy {
namespace storage {

/* Storage manager class -- inherited from storage_management_ops virtual class */
class storage_manager : public storage_management_ops {
 public:
  storage_manager() = default;

  virtual ~storage_manager() = default;

  /**
   * @brief Create partition
   * @param block_id Partition name
   * @param path Block path
   * @param chain Chain block names
   * @param role Block role
   * @param next_block_name Next block's name
   */
  void create_partition(const std::string &block_id,
                        const std::string &type,
                        const std::string &name,
                        const std::string &metadata,
                        const std::map<std::string, std::string> &conf) override;

  /**
   * @brief Destroy partition
   * @param block_name Block name
   */
  void destroy_partition(const std::string &block_name) override;

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

  /**
   * @brief Setup a replica chian
   * @param block_id Block identifier
   * @param path File path
   * @param chain Replica chain
   * @param role Chain role
   * @param next_block_id Next block identifier
   */
  void setup_chain(const std::string &block_id,
                   const std::string &path,
                   const std::vector<std::string> &chain,
                   int32_t role,
                   const std::string &next_block_id) override;

  /**
   * @brief Update partition data and metadata
   * @param block_name Block name
   * @param partition_name New partition name
   * @param partition_metadata New partition metadata
   */

  void update_partition(const std::string &block_name,
                        const std::string &partition_name,
                        const std::string &partition_metadata) override;
};

}
}

#endif //JIFFY_KV_MANAGER_H
