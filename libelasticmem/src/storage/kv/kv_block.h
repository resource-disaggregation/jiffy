#ifndef ELASTICMEM_KV_SERVICE_SHARD_H
#define ELASTICMEM_KV_SERVICE_SHARD_H

#include <libcuckoo/cuckoohash_map.hh>

#include <string>
#include "serializer/serde.h"
#include "serializer/binary_serde.h"
#include "../block.h"
#include "../../persistent/persistent_service.h"
#include "../chain_module.h"
#include "kv_hash.h"

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

// TODO: This should really be a map... maintaining the ids is tedious!
extern std::vector<block_op> KV_OPS;

enum kv_op_id : int32_t {
  exists = 0,
  get = 1,
  keys = 2, // TODO: We should not support multi-key operations since we do not provide any guarantees
  num_keys = 3, // TODO: We should not support multi-key operations since we do not provide any guarantees
  put = 4,
  remove = 5,
  update = 6,
  zget = 7,
  zput = 8,
  zremove = 9,
  zupdate = 10
};

class kv_block : public chain_module {
 public:
  explicit kv_block(const std::string &block_name,
                    std::size_t capacity = 134217728, // 128 MB; TODO: hardcoded default
                    double threshold_lo = 0.05,
                    double threshold_hi = 0.95,
                    const std::string &directory_host = "127.0.0.1",
                    int directory_port = 9090,
                    std::shared_ptr<persistent::persistent_service> persistent = std::make_shared<noop_store>(),
                    std::string local_storage_prefix = "/tmp",
                    std::shared_ptr<serializer> ser = std::make_shared<binary_serializer>(),
                    std::shared_ptr<deserializer> deser = std::make_shared<binary_deserializer>());

  std::string exists(const key_type &key, bool redirect = false);

  std::string put(const key_type &key, const value_type &value, bool redirect = false);

  value_type get(const key_type &key, bool redirect = false);

  std::string update(const key_type &key, const value_type &value, bool redirect = false);

  std::string remove(const key_type &key, bool redirect = false);

  void keys(std::vector<std::string> &keys);

  std::size_t size() const;

  bool empty() const;

  void run_command(std::vector<std::string> &_return, int oid, const std::vector<std::string> &args) override;

  void load(const std::string &remote_storage_prefix, const std::string &path) override;

  void flush(const std::string &remote_storage_prefix, const std::string &path) override;

  std::size_t storage_capacity() override;

  std::size_t storage_size() override;

  void reset() override;
  void forward_all() override;

  void export_slots() override;

 private:
  bool overload();

  bool underload();

  hash_table_type block_;

  std::string directory_host_;
  int directory_port_;

  std::shared_ptr<persistent::persistent_service> persistent_;
  std::string local_storage_prefix_;
  std::shared_ptr<serializer> ser_;
  std::shared_ptr<deserializer> deser_;
  std::atomic<size_t> bytes_;
  std::size_t capacity_;
  double threshold_lo_;
  double threshold_hi_;
  std::atomic<bool> splitting_;
  std::atomic<bool> merging_;
};

}
}

#endif //ELASTICMEM_KV_SERVICE_SHARD_H
