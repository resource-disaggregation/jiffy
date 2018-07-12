#ifndef MMUX_KV_MANAGER_H
#define MMUX_KV_MANAGER_H

#include <vector>
#include <string>

#include "../storage_management_ops.h"

namespace mmux {
namespace storage {

class storage_manager : public storage_management_ops {
 public:
  storage_manager() = default;
  void setup_block(const std::string &block_name,
                   const std::string &path,
                   int32_t slot_begin,
                   int32_t slot_end,
                   const std::vector<std::string> &chain,
                   bool auto_scale,
                   int32_t role,
                   const std::string &next_block_name) override;
  std::string path(const std::string &block_name) override;
  std::pair<int32_t, int32_t> slot_range(const std::string &block_name) override;
  void load(const std::string &block_name, const std::string &backing_path) override;
  void sync(const std::string &block_name, const std::string &backing_path) override;
  void dump(const std::string &block_name, const std::string &backing_path) override;
  void reset(const std::string &block_name) override;
  size_t storage_capacity(const std::string &block_name) override;
  size_t storage_size(const std::string &block_name) override;
  void resend_pending(const std::string &block_name) override;
  void forward_all(const std::string &block_name) override;
  void export_slots(const std::string &block_name) override;
  void set_exporting(const std::string &block_name,
                     const std::vector<std::string> &target_block,
                     int32_t slot_begin,
                     int32_t slot_end) override;
  void setup_and_set_importing(const std::string &block_name,
                               const std::string &path,
                               int32_t slot_begin,
                               int32_t slot_end,
                               const std::vector<std::string> &chain,
                               int32_t role,
                               const std::string &next_block_name) override;
  void set_regular(const std::string &block_name, int32_t slot_begin, int32_t slot_end) override;
  virtual void set_importing(const std::string &block_name, int32_t slot_begin, int32_t slot_end);
};

}
}

#endif //MMUX_KV_MANAGER_H
