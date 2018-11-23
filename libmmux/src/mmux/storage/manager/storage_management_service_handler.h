#ifndef MMUX_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H
#define MMUX_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H

#include "storage_management_service.h"
#include "../block.h"
#include "../chain_module.h"

namespace mmux {
namespace storage {
/* */
class storage_management_service_handler : public storage_management_serviceIf {
 public:

  /**
   * @brief
   * @param blocks
   */

  explicit storage_management_service_handler(std::vector<std::shared_ptr<chain_module>> &blocks);

  /**
   * @brief
   * @param _return
   * @param block_id
   */

  void get_path(std::string &_return, int32_t block_id) override;

  /**
   * @brief
   * @param block_id
   * @param backing_path
   */

  void sync(int32_t block_id, const std::string &backing_path) override;

  /**
   * @brief
   * @param block_id
   * @param backing_path
   */

  void dump(int32_t block_id, const std::string &backing_path) override;

  /**
   * @brief
   * @param block_id
   * @param backing_path
   */

  void load(int32_t block_id, const std::string &backing_path) override;

  /**
   * @brief
   * @param block_id
   */

  void reset(int32_t block_id) override;

  /**
   * @brief
   * @param block_id
   * @return
   */

  int64_t storage_capacity(int32_t block_id) override;

  /**
   * @brief
   * @param block_id
   * @return
   */

  int64_t storage_size(int32_t block_id) override;

  /**
   * @brief
   * @param block_id
   */

  void resend_pending(int32_t block_id) override;

  /**
   * @brief
   * @param block_id
   */

  void forward_all(int32_t block_id) override;

  /**
   * @brief
   * @param block_id
   * @param path
   * @param slot_begin
   * @param slot_end
   * @param chain
   * @param auto_scale
   * @param chain_role
   * @param next_block_name
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
   * @brief
   * @param _return
   * @param block_id
   */

  void slot_range(rpc_slot_range &_return, int32_t block_id) override;

  /**
   * @brief
   * @param block_id
   * @param target_block
   * @param slot_begin
   * @param slot_end
   */

  void set_exporting(int32_t block_id,
                     const std::vector<std::string> &target_block,
                     int32_t slot_begin,
                     int32_t slot_end) override;

  /**
   * @brief
   * @param block_id
   * @param slot_begin
   * @param slot_end
   */

  void set_importing(int32_t block_id,
                     int32_t slot_begin,
                     int32_t slot_end) override;

  /**
   * @brief
   * @param block_id
   * @param path
   * @param slot_begin
   * @param slot_end
   * @param chain
   * @param chain_role
   * @param next_block_name
   */

  void setup_and_set_importing(int32_t block_id,
                               const std::string &path,
                               int32_t slot_begin,
                               int32_t slot_end,
                               const std::vector<std::string> &chain,
                               int32_t chain_role,
                               const std::string &next_block_name) override;

  /**
   * @brief
   * @param block_id
   */

  void export_slots(int32_t block_id) override;

  /**
   * @brief
   * @param block_id
   * @param slot_begin
   * @param slot_end
   */

  void set_regular(int32_t block_id, int32_t slot_begin, int32_t slot_end) override;

 private:

  /**
   * @brief
   * @param e
   * @return
   */

  storage_management_exception make_exception(std::exception &e);

  /**
   * @brief
   * @param msg
   * @return
   */

  storage_management_exception make_exception(const std::string &msg);

  /**
   * @brief
   */
   
  std::vector<std::shared_ptr<chain_module>> &blocks_;
};

}
}

#endif //MMUX_KV_MANAGEMENT_RPC_SERVICE_HANDLER_H
