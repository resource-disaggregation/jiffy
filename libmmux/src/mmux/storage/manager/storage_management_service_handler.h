#ifndef MMUX_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H
#define MMUX_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H

#include "storage_management_service.h"
#include "../block.h"
#include "../chain_module.h"

namespace mmux {
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
                   int32_t slot_begin,
                   int32_t slot_end,
                   const std::vector<std::string> &chain,
                   bool auto_scale,
                   int32_t chain_role,
                   const std::string &next_block_name) override;

  /**
   * @brief Fetch slot range of block
   * @param _return Slot range to be returned
   * @param block_id Block identifier
   */

  void slot_range(rpc_slot_range &_return, int32_t block_id) override;

  /**
   * @brief Setup exporting target and slot range
   * @param block_id Block identifier
   * @param target_block Exporting target blocks
   * @param slot_begin Export begin slot
   * @param slot_end Export end slot
   */

  void set_exporting(int32_t block_id,
                     const std::vector<std::string> &target_block,
                     int32_t slot_begin,
                     int32_t slot_end) override;

  /**
   * @brief Setup importing slot range
   * @param block_id Block identifier
   * @param slot_begin Importing begin slot
   * @param slot_end Importing end slot
   */

  void set_importing(int32_t block_id,
                     int32_t slot_begin,
                     int32_t slot_end) override;

  /**
   * @brief Setup the block and set importing slot range
   * @param block_id Block identifier
   * @param path Block path
   * @param slot_begin Importing begin slot
   * @param slot_end Importing end slot
   * @param chain Chain block names
   * @param chain_role Chain role
   * @param next_block_name Next block name
   */

  void setup_and_set_importing(int32_t block_id,
                               const std::string &path,
                               int32_t slot_begin,
                               int32_t slot_end,
                               const std::vector<std::string> &chain,
                               int32_t chain_role,
                               const std::string &next_block_name) override;

  /**
   * @brief Export slots
   * @param block_id Block identifier
   */

  void export_slots(int32_t block_id) override;

  /**
   * @brief Set the block to be regular after exporting
   * @param block_id Block identifier
   * @param slot_begin Begin slot
   * @param slot_end End slot
   */

  void set_regular(int32_t block_id, int32_t slot_begin, int32_t slot_end) override;

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

#endif //MMUX_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H