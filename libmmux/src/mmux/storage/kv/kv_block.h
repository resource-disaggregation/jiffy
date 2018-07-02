#ifndef MMUX_KV_SERVICE_SHARD_H
#define MMUX_KV_SERVICE_SHARD_H

#include <libcuckoo/cuckoohash_map.hh>

#include <string>
#include "serde/serde.h"
#include "serde/binary_serde.h"
#include "../block.h"
#include "../../persistent/persistent_service.h"
#include "../chain_module.h"
#include "kv_hash.h"
#include "serde/csv_serde.h"

namespace mmux {
namespace storage {

typedef std::string binary; // Since thrift translates binary to string
typedef binary key_type;
typedef binary value_type;

extern std::vector<block_op> KV_OPS;

enum kv_op_id : int32_t {
  exists = 0,
  get = 1,
  keys = 2, // TODO: We should not support multi-key operations since we do not provide any guarantees
  num_keys = 3, // TODO: We should not support multi-key operations since we do not provide any guarantees
  put = 4,
  remove = 5,
  update = 6,
  lock = 7,
  unlock = 8,
  locked_data_in_slot_range = 9,
  locked_get = 10,
  locked_put = 11,
  locked_remove = 12,
  locked_update = 13,
};

class kv_block : public chain_module {
 public:
  explicit kv_block(const std::string &block_name,
                    std::size_t capacity = 134217728, // 128 MB; TODO: hardcoded default
                    double threshold_lo = 0.05,
                    double threshold_hi = 0.95,
                    const std::string &directory_host = "127.0.0.1",
                    int directory_port = 9090,
                    std::shared_ptr<serde> ser = std::make_shared<csv_serde>());

  std::string exists(const key_type &key, bool redirect = false);

  std::string put(const key_type &key, const value_type &value, bool redirect = false);

  std::string locked_put(const key_type &key, const value_type &value, bool redirect = false);

  value_type get(const key_type &key, bool redirect = false);

  std::string locked_get(const key_type &key, bool redirect = false);

  std::string update(const key_type &key, const value_type &value, bool redirect = false);

  std::string locked_update(const key_type &key, const value_type &value, bool redirect = false);

  std::string remove(const key_type &key, bool redirect = false);

  std::string locked_remove(const key_type &key, bool redirect = false);

  void keys(std::vector<std::string> &keys);

  void locked_get_data_in_slot_range(std::vector<std::string> &data,
                                     int32_t slot_begin,
                                     int32_t slot_end,
                                     int32_t num_keys);

  std::string lock();

  std::string unlock();

  bool is_locked();

  std::size_t size() const;

  bool empty() const;

  void run_command(std::vector<std::string> &_return, int oid, const std::vector<std::string> &args) override;

  bool is_dirty() const;

  void load(const std::string &path) override;

  bool sync(const std::string &path) override;

  bool dump(const std::string &path) override;

  std::size_t storage_capacity() override;

  std::size_t storage_size() override;

  void reset() override;
  void forward_all() override;

  void export_slots() override;

 private:
  bool overload();

  bool underload();

  hash_table_type block_;
  locked_hash_table_type locked_block_;

  std::string directory_host_;
  int directory_port_;

  std::shared_ptr<serde> ser_;
  std::atomic<size_t> bytes_;
  std::size_t capacity_;
  double threshold_lo_;
  double threshold_hi_;
  std::atomic<bool> splitting_;
  std::atomic<bool> merging_;

  std::atomic<bool> dirty_;
};

}
}

#endif //MMUX_KV_SERVICE_SHARD_H
