#ifndef MMUX_KV_MANAGER_H
#define MMUX_KV_MANAGER_H

#include <vector>
#include <string>

#include "../storage_management_ops.h"

namespace mmux {
namespace storage {
/* */
class storage_manager : public storage_management_ops {
 public:
  storage_manager() = default;

  /**
   * @brief
   * @param block_name
   * @param path
   * @param slot_begin
   * @param slot_end
   * @param chain
   * @param auto_scale
   * @param role
   * @param next_block_name
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
   * @brief
   * @param block_name
   * @return
   */

  std::string path(const std::string &block_name) override;

  /**
   * @brief
   * @param block_name
   * @return
   */

  std::pair<int32_t, int32_t> slot_range(const std::string &block_name) override;

  /**
   * @brief
   * @param block_name
   * @param backing_path
   */

  void load(const std::string &block_name, const std::string &backing_path) override;

  /**
   * @brief
   * @param block_name
   * @param backing_path
   */

  void sync(const std::string &block_name, const std::string &backing_path) override;

  /**
   * @brief
   * @param block_name
   * @param backing_path
   */

  void dump(const std::string &block_name, const std::string &backing_path) override;

  /**
   * @brief
   * @param block_name
   */

  void reset(const std::string &block_name) override;

  /**
   * @brief
   * @param block_name
   * @return
   */

  size_t storage_capacity(const std::string &block_name) override;

  /**
   * @brief
   * @param block_name
   * @return
   */

  size_t storage_size(const std::string &block_name) override;

  /**
   * @brief
   * @param block_name
   */

  void resend_pending(const std::string &block_name) override;

  /**
   * @brief
   * @param block_name
   */

  void forward_all(const std::string &block_name) override;

  /**
   * @brief
   * @param block_name
   */

  void export_slots(const std::string &block_name) override;

  /**
   * @brief
   * @param block_name
   * @param target_block
   * @param slot_begin
   * @param slot_end
   */

  void set_exporting(const std::string &block_name,
                     const std::vector<std::string> &target_block,
                     int32_t slot_begin,
                     int32_t slot_end) override;

  /**
   * @brief
   * @param block_name
   * @param path
   * @param slot_begin
   * @param slot_end
   * @param chain
   * @param role
   * @param next_block_name
   */

  void setup_and_set_importing(const std::string &block_name,
                               const std::string &path,
                               int32_t slot_begin,
                               int32_t slot_end,
                               const std::vector<std::string> &chain,
                               int32_t role,
                               const std::string &next_block_name) override;

  /**
   * @brief
   * @param block_name
   * @param slot_begin
   * @param slot_end
   */

  void set_regular(const std::string &block_name, int32_t slot_begin, int32_t slot_end) override;

  /**
   * @brief
   * @param block_name
   * @param slot_begin
   * @param slot_end
   */

  virtual void set_importing(const std::string &block_name, int32_t slot_begin, int32_t slot_end);
};

}
}

#endif //MMUX_KV_MANAGER_H
