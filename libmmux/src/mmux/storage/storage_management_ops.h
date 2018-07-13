#ifndef MMUX_KV_SERVICE_H
#define MMUX_KV_SERVICE_H

#include <cstddef>
#include <string>

#include "../persistent/persistent_service.h"
#include "block.h"

namespace mmux {
namespace storage {

class storage_management_ops {
 public:
  virtual void setup_block(const std::string &block_name,
                           const std::string &path,
                           int32_t slot_begin,
                           int32_t slot_end,
                           const std::vector<std::string> &chain,
                           bool auto_scale,
                           int32_t role,
                           const std::string &next_block_name) = 0;

  virtual std::pair<int32_t, int32_t> slot_range(const std::string &block_name) = 0;

  virtual std::string path(const std::string &block_name) = 0;

  virtual void load(const std::string &block_name, const std::string &backing_path) = 0;

  virtual void sync(const std::string &block_name, const std::string &backing_path) = 0;

  virtual void dump(const std::string &block_name, const std::string &backing_path) = 0;

  virtual void reset(const std::string &block_name) = 0;

  virtual void set_exporting(const std::string &block_name,
                             const std::vector<std::string> &target_block,
                             int32_t slot_begin,
                             int32_t slot_end) = 0;

  virtual void setup_and_set_importing(const std::string &block_name,
                                       const std::string &path,
                                       int32_t slot_begin,
                                       int32_t slot_end,
                                       const std::vector<std::string> &chain,
                                       int32_t role,
                                       const std::string &next_block_name) = 0;

  virtual void set_importing(const std::string &block_name,
                             int32_t slot_begin,
                             int32_t slot_end) = 0;

  virtual void export_slots(const std::string &block_name) = 0;

  virtual void set_regular(const std::string &block_name, int32_t slot_begin, int32_t slot_end) = 0;

  virtual std::size_t storage_capacity(const std::string &block_name) = 0;

  virtual std::size_t storage_size(const std::string &block_name) = 0;

  virtual void resend_pending(const std::string &block_name) = 0;

  virtual void forward_all(const std::string &block_name) = 0;

};

}
}

#endif //MMUX_KV_SERVICE_H
