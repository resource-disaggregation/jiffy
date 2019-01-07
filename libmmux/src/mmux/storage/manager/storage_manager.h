#ifndef MMUX_KV_MANAGER_H
#define MMUX_KV_MANAGER_H

#include <vector>
#include <string>

#include "../storage_management_ops.h"

namespace mmux {
namespace storage {
/* Storage manager class
 * Inherited from storage_management_ops virtual class */
class storage_manager : public storage_management_ops {
 public:
  storage_manager() = default;

  /**
   * @brief Setup block
   * @param block_name Block name
   * @param path Block path
   * @param slot_begin Begin Slot
   * @param slot_end End Slot
   * @param chain Chain block names
   * @param auto_scale Bool value, true if auto_scale is on
   * @param role Block role
   * @param next_block_name Next block's name
   */

  void setup_block(const std::string &block_name,
                   const std::string &path,
                   int32_t slot_begin,
                   int32_t slot_end,
                   const std::vector<std::string> &chain,
                   bool auto_scale,
                   int32_t role,
                   const std::string &next_block_name) override;

  /**
   * @brief Fetch block path
   * @param block_name Block name
   * @return Block path
   */

  std::string path(const std::string &block_name) override;

  /**
   * @brief Fetch block slot range
   * @param block_name Block name
   * @return Block slot range
   */

  std::pair<int32_t, int32_t> slot_range(const std::string &block_name) override;

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

  /**
   * @brief Export slot range
   * @param block_name Block name
   */

  void export_slots(const std::string &block_name) override;

  /**
   * @brief Set exporting slot range and exporting target
   * @param block_name Block name
   * @param target_block Exporting target blocks
   * @param slot_begin Exporting begin slot
   * @param slot_end Exporting end slot
   */

  void set_exporting(const std::string &block_name,
                     const std::vector<std::string> &target_block,
                     int32_t slot_begin,
                     int32_t slot_end) override;

  /**
   * @brief Setup the block and set importing slot range
   * @param block_name Block name
   * @param path Block path
   * @param slot_begin Importing begin slot
   * @param slot_end Importing end slot
   * @param chain Chain block names
   * @param role Block role
   * @param next_block_name Next block's name
   */

  void setup_and_set_importing(const std::string &block_name,
                               const std::string &path,
                               int32_t slot_begin,
                               int32_t slot_end,
                               const std::vector<std::string> &chain,
                               int32_t role,
                               const std::string &next_block_name) override;

  /**
   * @brief Set block to regular after exporting
   * @param block_name Block name
   * @param slot_begin Begin slot
   * @param slot_end End slot
   */

  void set_regular(const std::string &block_name, int32_t slot_begin, int32_t slot_end) override;

  /**
   * @brief Virtual function of set importing
   * @param block_name Block name
   * @param slot_begin Importing begin slot
   * @param slot_end Importing end slot
   */

  virtual void set_importing(const std::string &block_name, int32_t slot_begin, int32_t slot_end);
};

}
}

#endif //MMUX_KV_MANAGER_H