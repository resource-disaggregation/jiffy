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
 * @brief Key value block supported operations
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
   * @brief Constructor, lock map created by assigning block_.lock_table
   * @param block_name Block name
   * @param capacity Block capacity
   * @param threshold_lo low threshold
   * @param threshold_hi high threshold
   * @param directory_host Directory host
   * @param directory_port Directory port number
   * @param ser Serialization and deserialization method, csv for default
   */

  explicit kv_block(const std::string &block_name,
                    std::size_t capacity = 134217728, // 128 MB; TODO: hardcoded default
                    double threshold_lo = 0.05,
                    double threshold_hi = 0.95,
                    const std::string &directory_host = "127.0.0.1",
                    int directory_port = 9090,
                    std::shared_ptr<serde> ser = std::make_shared<csv_serde>());

  /**
   * @brief Check if hash map contains key
   * @param key Key
   * @param redirect Bool value to choose whether to indirect to the destination block when block is in repartitioning
   * @return String of key status
   */

  std::string exists(const key_type &key, bool redirect = false);

  /**
   * @brief Put new key value pair
   * @param key Key
   * @param value Value
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning, false for default
   * @return Put status string
   */

  std::string put(const key_type &key, const value_type &value, bool redirect = false);

  /**
   * @brief Put new key value pair in locked block
   * @param key Key
   * @param value Value
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning, false for default
   * @return Locked put status string
   */

  std::string locked_put(const key_type &key, const value_type &value, bool redirect = false);

  /**
   * @brief Insert with the maximum value for specified key
   * @param key Key
   * @param value Value
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning, false for default
   * @return Upsert status string
   */

  std::string upsert(const key_type &key, const value_type &value, bool redirect = false);

  /**
   * @brief Insert with the maximum value for specified key in locked block
   * @param key Key
   * @param value Value
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning, false for default
   * @return Locked upsert status string
   */

  std::string locked_upsert(const key_type &key, const value_type &value, bool redirect = false);

  /**
   * @brief Get value for specified key
   * @param key Key
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning, false for default
   * @return Get status string
   */

  value_type get(const key_type &key, bool redirect = false);

  /**
   * @brief Get value for specified key from locked block
   * @param key Key
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning, false for default
   * @return Locked get status string
   */

  std::string locked_get(const key_type &key, bool redirect = false);

  /**
   * @brief Update the value for specified key
   * @param key Key
   * @param value Value
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning, false for default
   * @return Update status string
   */

  std::string update(const key_type &key, const value_type &value, bool redirect = false);

  /**
   * @brief Update the value for specified key in locked block
   * @param key Key
   * @param value Value
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning, false for default
   * @return Locked update status string
   */

  std::string locked_update(const key_type &key, const value_type &value, bool redirect = false);

  /**
   * @brief Remove value for specified key
   * @param key Key
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning, false for default
   * @return Remove status string
   */

  std::string remove(const key_type &key, bool redirect = false);

  /**
   * @brief Remove value for specified key in locked block
   * @param key Key
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning, false for default
   * @return Locked remove status
   */

  std::string locked_remove(const key_type &key, bool redirect = false);

  /**
   * @brief Return keys in locked block
   * @param keys Vector to store keys
   * TODO to be removed?
   */

  void keys(std::vector<std::string> &keys);

  /**
   * @brief Fetch num_keys data from keys which lie in slot range
   * @param data Data vector to be fetched
   * @param slot_begin Slot begin value
   * @param slot_end Slot end value
   * @param num_keys Key numbers to be fetched
   */

  void locked_get_data_in_slot_range(std::vector<std::string> &data,
                                     int32_t slot_begin,
                                     int32_t slot_end,
                                     int32_t num_keys);

  /**
   * @brief Active lock block
   * @return Lock status string, export target string if block exporting
   */

  std::string lock();

  /**
   * @brief Unlock the lock block
   * @return Unlock status string
   */

  std::string unlock();

  /**
   * @brief Check if key value block is locked
   * @return Bool value, true if locked
   */

  bool is_locked();

  /**
   * @brief Fetch block size
   * @return Block size
   */

  std::size_t size() const;

  /**
   * @brief Check if block is empty
   * @return Bool value, true if empty
   */

  bool empty() const;

  /**
   * @brief Run particular command on key value block
   * Check whether block hash range overloads or underloads
   * @param _return Return status to be fetched
   * @param oid Operation id
   * @param args Command arguments
   */

  void run_command(std::vector<std::string> &_return, int oid, const std::vector<std::string> &args) override;

  /**
   * @brief Atomically check dirty bit
   * @return Bool value, true if block is dirty
   */

  bool is_dirty() const;

  /**
   * @brief Load persistent data into the block, lock the block while doing this
   * @param path Persistent storage path
   */

  void load(const std::string &path) override;

  /**
   * @brief If dirty, synchronize persistent storage and block
   * @param path Persistent storage path
   * @return Bool
   */

  bool sync(const std::string &path) override;

  /**
   * @brief Flush the block if dirty and clear the block
   * @param path Persistent storage path
   * @return Bool value, true if block successfully dumped
   */

  bool dump(const std::string &path) override;

  /**
   * @brief Fetch block storage capacity
   * @return Block storage capacity
   */

  std::size_t storage_capacity() override;

  /**
   * @brief Fetch block storage size, total size of all the keys and values
   * @return Block storage size
   */

  std::size_t storage_size() override;

  /**
   * @brief Reset the block, while occupying metadata_mtx_
   */

  void reset() override;

  /**
   * @brief TODO
   */

  void forward_all() override;

  /**
   * @brief
   */

  void export_slots() override;

 private:
  /**
   * @brief Check if block is overloaded
   * @return Bool value, true if block size overflows the high threshold capacity
   */

  bool overload();

  /**
   * @brief Check if block is underloaded
   * @return Bool value, true if block size underflows the low threshold capacity
   */

  bool underload();

  /* Cuckoohash_map block allocation */
  hash_table_type block_;

  /* Locked cuckoohash_map block */
  locked_hash_table_type locked_block_;

  /* Directory host number */
  std::string directory_host_;

  /* Directory port number */
  int directory_port_;

  /* Serialization & deserialization option */
  std::shared_ptr<serde> ser_;

  /* Atomic value to collect the sum of key size and value size */
  std::atomic<size_t> bytes_;

  /* Key value block capacity */
  std::size_t capacity_;

  /**
   * @brief The two threshold to determine whether the block
   * is overloaded or underloaded
   */
  /* Low threshold */
  double threshold_lo_;
  /* High threshold */
  double threshold_hi_;

  /* Atomic bool for block hash slot range splitting */
  std::atomic<bool> splitting_;

  /* Atomic bool for block hash slot range merging */
  std::atomic<bool> merging_;

  /* Atomic block dirty bit */
  std::atomic<bool> dirty_;
};

}
}

#endif //MMUX_KV_SERVICE_SHARD_H
