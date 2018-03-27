#ifndef ELASTICMEM_KV_MANAGER_H
#define ELASTICMEM_KV_MANAGER_H

#include <vector>
#include <string>

#include "../storage_management_ops.h"

namespace elasticmem {
namespace storage {

class storage_manager : public storage_management_ops {
 public:
  storage_manager() = default;
  void setup_block(const std::string &block_name,
                     const std::string &path,
                     int32_t role,
                     const std::string &next_block_name) override;
  std::string path(const std::string& block_name) override;
  void load(const std::string &block_name,
            const std::string &persistent_path_prefix,
            const std::string &path) override;
  void flush(const std::string &block_name,
             const std::string &persistent_path_prefix,
             const std::string &path) override;
  void reset(const std::string &block_name) override;
  size_t storage_capacity(const std::string &block_name) override;
  size_t storage_size(const std::string &block_name) override;
  void resend_pending(const std::string &block_name) override;
};

}
}

#endif //ELASTICMEM_KV_MANAGER_H
