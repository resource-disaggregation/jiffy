#ifndef ELASTICMEM_BLOCK_MANAGEMENT_OPS_H
#define ELASTICMEM_BLOCK_MANAGEMENT_OPS_H

#include <string>

namespace elasticmem {
namespace storage {

class block_management_ops {
 public:
  virtual void load(const std::string &remote_storage_prefix, const std::string &path) = 0;

  virtual void flush(const std::string &remote_storage_prefix, const std::string &path) = 0;

  virtual std::size_t storage_capacity() = 0;

  virtual std::size_t storage_size() = 0;

  virtual void clear() = 0;
};

}
}

#endif //ELASTICMEM_BLOCK_MANAGEMENT_OPS_H
