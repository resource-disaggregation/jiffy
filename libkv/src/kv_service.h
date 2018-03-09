#ifndef ELASTICMEM_KV_SERVICE_H
#define ELASTICMEM_KV_SERVICE_H

#include <cstddef>
#include <string>

namespace elasticmem {
namespace kv {

class kv_service {
 public:
  typedef std::string binary; // Since thrift translates binary to string
  typedef binary key_type;
  typedef binary value_type;

  virtual void put(const key_type &key, const value_type &value) = 0;

  virtual value_type const &get(const key_type &key) = 0;

  virtual value_type const &update(const key_type &key, const value_type &value) = 0;

  virtual void remove(const key_type &key) = 0;
};

class kv_management_service {
  virtual void clear() = 0;

  virtual std::size_t size() = 0;

  virtual std::size_t capacity() = 0;

  virtual std::size_t num_entries() = 0;
};

}
}

#endif //ELASTICMEM_KV_SERVICE_H
