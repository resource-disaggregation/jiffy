#ifndef ELASTICMEM_KV_SERVICE_H
#define ELASTICMEM_KV_SERVICE_H

#include <cstddef>
#include <string>

#include <persistent_service.h>

namespace elasticmem {
namespace kv {

typedef std::string binary; // Since thrift translates binary to string
typedef binary key_type;
typedef binary value_type;

class kv_management_service {
 public:
  virtual void load(const std::string &block_name,
                    const std::string &persistent_path_prefix,
                    const std::string &path) = 0;

  virtual void flush(const std::string &block_name,
                     const std::string &persistent_path_prefix,
                     const std::string &path) = 0;

  virtual void clear(const std::string &block_name) = 0;

  virtual std::size_t storage_capacity(const std::string &block_name) = 0;

  virtual std::size_t storage_size(const std::string &block_name) = 0;
};

}
}

#endif //ELASTICMEM_KV_SERVICE_H
