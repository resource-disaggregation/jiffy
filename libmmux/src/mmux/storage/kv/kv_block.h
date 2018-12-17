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
/**
 * @brief
 */
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
  upsert = 14,
  locked_upsert = 15
};
/* Key value block structure class, inherited from chain module */
class kv_block : public chain_module {
 public:
  /**
   * @brief Constructor
   * @param block_name Block name
   * @param capacity Block capacity
   * @param threshold_lo low threshold
   * @param threshold_hi high threshold
   * @param directory_host Directory host
   * @param directory_port Directory port number
   * @param ser Ser
   */

  explicit kv_block(const std::string &block_name,
                    std::size_t capacity = 134217728, // 128 MB; TODO: hardcoded default
                    double threshold_lo = 0.05,
                    double threshold_hi = 0.95,
                    const std::string &directory_host = "127.0.0.1",
                    int directory_port = 9090,
                    std::shared_ptr<serde> ser = std::make_shared<csv_serde>());

  /**
   * @brief
   * @param key
   * @param redirect
   * @return
   */

  std::string exists(const key_type &key, bool redirect = false);

  /**
   * @brief
   * @param key
   * @param value
   * @param redirect
   * @return
   */

  std::string put(const key_type &key, const value_type &value, bool redirect = false);

  /**
   * @brief
   * @param key
   * @param value
   * @param redirect
   * @return
   */

  std::string locked_put(const key_type &key, const value_type &value, bool redirect = false);

  /**
   * @brief
   * @param key
   * @param value
   * @param redirect
   * @return
   */

  std::string upsert(const key_type &key, const value_type &value, bool redirect = false);

  /**
   * @brief
   * @param key
   * @param value
   * @param redirect
   * @return
   */

  std::string locked_upsert(const key_type &key, const value_type &value, bool redirect = false);

  /**
   * @brief
   * @param key
   * @param redirect
   * @return
   */

  value_type get(const key_type &key, bool redirect = false);

  /**
   * @brief
   * @param key
   * @param redirect
   * @return
   */

  std::string locked_get(const key_type &key, bool redirect = false);

  /**
   * @brief
   * @param key
   * @param value
   * @param redirect
   * @return
   */

  std::string update(const key_type &key, const value_type &value, bool redirect = false);

  /**
   * @brief
   * @param key
   * @param value
   * @param redirect
   * @return
   */

  std::string locked_update(const key_type &key, const value_type &value, bool redirect = false);

  /**
   * @brief
   * @param key
   * @param redirect
   * @return
   */

  std::string remove(const key_type &key, bool redirect = false);

  /**
   * @brief
   * @param key
   * @param redirect
   * @return
   */

  std::string locked_remove(const key_type &key, bool redirect = false);

  /**
   * @brief
   * @param keys
   */

  void keys(std::vector<std::string> &keys);

  /**
   * @brief
   * @param data
   * @param slot_begin
   * @param slot_end
   * @param num_keys
   */

  void locked_get_data_in_slot_range(std::vector<std::string> &data,
                                     int32_t slot_begin,
                                     int32_t slot_end,
                                     int32_t num_keys);

  /**
   * @brief
   * @return
   */

  std::string lock();

  /**
   * @brief
   * @return
   */

  std::string unlock();

  /**
   * @brief
   * @return
   */

  bool is_locked();

  /**
   * @brief
   * @return
   */

  std::size_t size() const;

  /**
   * @brief
   * @return
   */

  bool empty() const;

  /**
   * @brief
   * @param _return
   * @param oid
   * @param args
   */

  void run_command(std::vector<std::string> &_return, int oid, const std::vector<std::string> &args) override;

  /**
   * @brief
   * @return
   */

  bool is_dirty() const;

  /**
   * @brief
   * @param path
   */

  void load(const std::string &path) override;

  /**
   * @brief
   * @param path
   * @return
   */

  bool sync(const std::string &path) override;

  /**
   * @brief
   * @param path
   * @return
   */

  bool dump(const std::string &path) override;

  /**
   * @brief
   * @return
   */

  std::size_t storage_capacity() override;

  /**
   * @brief
   * @return
   */

  std::size_t storage_size() override;

  /**
   * @brief
   */

  void reset() override;

  /**
   * @brief
   */

  void forward_all() override;

  /**
   * @brief
   */

  void export_slots() override;

 private:
  /**
   * @brief
   * @return
   */

  bool overload();

  /**
   * @brief
   * @return
   */

  bool underload();

  /* */
  hash_table_type block_;
  /* */
  locked_hash_table_type locked_block_;
  /* */
  std::string directory_host_;
  /* */
  int directory_port_;
  /* */
  std::shared_ptr<serde> ser_;
  /* */
  std::atomic<size_t> bytes_;
  /* */
  std::size_t capacity_;
  /* */
  double threshold_lo_;
  /* */
  double threshold_hi_;
  /* */
  std::atomic<bool> splitting_;
  /* */
  std::atomic<bool> merging_;
  /* */
  std::atomic<bool> dirty_;
};

}
}

#endif //MMUX_KV_SERVICE_SHARD_H
