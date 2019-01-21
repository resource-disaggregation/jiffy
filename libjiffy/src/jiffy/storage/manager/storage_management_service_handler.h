#ifndef JIFFY_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H
#define JIFFY_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H

#include "storage_management_service.h"
#include "jiffy/storage/partition.h"
#include "../chain_module.h"

namespace jiffy {
namespace storage {
/* Storage management service handler class,
 * inherited from storage_management_serviceIf */
class storage_management_service_handler : public storage_management_serviceIf {
 public:

  /**
   * @brief Constructor
   * @param blocks Blocks
   */

  explicit storage_management_service_handler(std::vector<std::shared_ptr<chain_module>> &blocks);

  /**
   * @brief Get block path
   * @param _return Block path
   * @param block_id Block identifier
   */

  void get_path(std::string &_return, int32_t block_id) override;

  /**
   * @brief Write data back to persistent storage
   * @param block_id Block identifier
   * @param backing_path Block backing path
   */

  void sync(int32_t block_id, const std::string &backing_path) override;

  /**
   * @brief Write data back to persistent storage and clear the block
   * @param block_id Block identifier
   * @param backing_path Block backing path
   */

  void dump(int32_t block_id, const std::string &backing_path) override;

  /**
   * @brief Load data block from persistent storage
   * @param block_id Block identifier
   * @param backing_path Block backing path
   */

  void load(int32_t block_id, const std::string &backing_path) override;

  /**
   * @brief Reset block
   * @param block_id Block identifier
   */

  void reset(int32_t block_id) override;

  /**
   * @brief Fetch storage capacity of block
   * @param block_id Block identifier
   * @return Block storage capacity
   */

  int64_t storage_capacity(int32_t block_id) override;

  /**
   * @brief Fetch storage size of block
   * @param block_id Block identifier
   * @return Block storage size
   */

  int64_t storage_size(int32_t block_id) override;

  /**
   * @brief Resend pending requests
   * @param block_id Block identifier
   */

  void resend_pending(int32_t block_id) override;

  /**
   * @brief Send all key values to next block
   * @param block_id Block identifier
   */

  void forward_all(int32_t block_id) override;

  /**
   * @brief Setup a block
   * @param block_id Block identifier
   * @param path Block path
   * @param slot_begin Block begin slot
   * @param slot_end Block end slot
   * @param chain Chain block names
   * @param auto_scale Bool value, true if auto_scale is on
   * @param chain_role Chain role
   * @param next_block_name Next block's name
   */

  void setup_block(int32_t block_id,
                   const std::string &path,
                   const std::string &partition_type,
                   const std::string &partition_name,
                   const std::string &partition_metadata,
                   const std::vector<std::string> &chain,
                   int32_t chain_role,
                   const std::string &next_block_name) override;

 private:

  /**
   * @brief Make exceptions
   * @param e exception
   * @return Storage management exceptions
   */

  storage_management_exception make_exception(std::exception &e);

  /**
   * @brief Make exceptions
   * @param msg Exception message
   * @return Storage management exceptions
   */

  storage_management_exception make_exception(const std::string &msg);

  /* Blocks */
  std::vector<std::shared_ptr<chain_module>> &blocks_;
};

}
}

#endif //JIFFY_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H
