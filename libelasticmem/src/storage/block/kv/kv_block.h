#ifndef ELASTICMEM_KV_SERVICE_SHARD_H
#define ELASTICMEM_KV_SERVICE_SHARD_H

#include <libcuckoo/cuckoohash_map.hh>

#include <string>
#include "serializer/serde.h"
#include "serializer/binary_serde.h"
#include "../block.h"
#include "../../../persistent/persistent_service.h"

namespace elasticmem {
namespace storage {

typedef std::string binary; // Since thrift translates binary to string
typedef binary key_type;
typedef binary value_type;

class noop_store : public persistent::persistent_service {
 public:
  void write(const std::string &, const std::string &) override {}
  void read(const std::string &, const std::string &) override {}
  void remove(const std::string &) override {}
};

extern std::vector<block_op> KV_OPS;

class kv_block : public block {
 public:
  typedef cuckoohash_map<key_type, value_type> hash_table_type;
  typedef hash_table_type::locked_table locked_hash_table_type;

  explicit kv_block(std::shared_ptr<persistent::persistent_service> persistent = std::make_shared<noop_store>(),
                    std::string local_storage_prefix = "/tmp",
                    std::shared_ptr<serializer> ser = std::make_shared<binary_serializer>(),
                    std::shared_ptr<deserializer> deser = std::make_shared<binary_deserializer>());

  void put(const key_type &key, const value_type &value);

  value_type get(const key_type &key);

  void update(const key_type &key, const value_type &value);

  void remove(const key_type &key);

  std::size_t size() const;

  bool empty() const;

  void run_command(std::vector<std::string>& _return, int oid, const std::vector<std::string> &args) override;

  void load(const std::string &remote_storage_prefix, const std::string &path) override;

  void flush(const std::string &remote_storage_prefix, const std::string &path) override;

  std::size_t storage_capacity() override;

  std::size_t storage_size() override;

  void clear() override;

 private:
  hash_table_type block_;
  std::shared_ptr<persistent::persistent_service> persistent_;
  std::string local_storage_prefix_;
  std::shared_ptr<serializer> ser_;
  std::shared_ptr<deserializer> deser_;
};

}
}

#endif //ELASTICMEM_KV_SERVICE_SHARD_H
