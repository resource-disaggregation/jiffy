#ifndef ELASTICMEM_KV_MANAGER_H
#define ELASTICMEM_KV_MANAGER_H

#include <vector>
#include <string>

#include "../kv_management_service.h"

namespace elasticmem {
namespace kv {

class kv_manager : public kv_management_service {
 public:
  kv_manager() = default;
  void load(const std::string &block_name,
            const std::string &persistent_path_prefix,
            const std::string &path) override;
  void flush(const std::string &block_name,
             const std::string &persistent_path_prefix,
             const std::string &path) override;
  void clear(const std::string &block_name) override;
  size_t storage_capacity(const std::string &block_name) override;
  size_t storage_size(const std::string &block_name) override;
};

}
}

#endif //ELASTICMEM_KV_MANAGER_H
