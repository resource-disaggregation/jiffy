#ifndef ELASTICMEM_KV_SERVICE_SHARD_H
#define ELASTICMEM_KV_SERVICE_SHARD_H

#include <libcuckoo/cuckoohash_map.hh>

#include "../kv_service.h"

namespace elasticmem {
namespace kv {

class kv_service_shard : public kv_service, public kv_management_service {
 public:
  kv_service_shard() = default;

  void put(const key_type &key, const value_type &value) override;

  value_type const &get(const key_type &key) override;

  value_type const &update(const key_type &key, const value_type &value) override;

  void remove(const key_type &key) override;

  void clear() override;

  std::size_t size() override;

  std::size_t capacity() override;

  std::size_t num_entries() override;

 private:
  cuckoohash_map<key_type, value_type> entries_;
};

}
}

#endif //ELASTICMEM_KV_SERVICE_SHARD_H
