#ifndef JIFFY_BTREE_SERVICE_SHARD_H
#define JIFFY_BTREE_SERVICE_SHARD_H

#include <string>
#include <jiffy/utils/property_map.h>
#include "../serde/serde.h"
#include "../serde/binary_serde.h"
#include "jiffy/storage/partition.h"
#include "jiffy/persistent/persistent_service.h"
#include "jiffy/storage/chain_module.h"
#include "btree_defs.h"
#include "../serde/csv_serde.h"

namespace jiffy {
namespace storage {

class btree_partition : public chain_module {
 public:

  explicit btree_partition(block_memory_manager *manager,
                           const std::string &name = "0_65536", //TODO need to fix this name
                           const std::string &metadata = "regular", //TODO need to fix this metadata
                           const utils::property_map &conf = {},
                           const std::string &directory_host = "localhost",
                           const int directory_port = 9091);

  /**
   * @brief Virtual destructor
   */
  virtual ~hash_table_partition() = default;

  /**
   * @brief Check if hash map contains key
   * @param key Key
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning
   * @return String of key status
   */

  std::string exists(const key_type &key, bool redirect = false);

  /**
   * @brief Put new key value pair
   * @param key Key
   * @param value Value
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning
   * @return Put status string
   */

  std::string put(const key_type &key, const value_type &value, bool redirect = false);

  /**
   * @brief Get value for specified key
   * @param key Key
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning
   * @return Get status string
   */

  value_type get(const key_type &key, bool redirect = false);

  /**
   * @brief Update the value for specified key
   * @param key Key
   * @param value Value
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning
   * @return Update status string
   */

  std::string update(const key_type &key, const value_type &value, bool redirect = false);

  /**
   * @brief Remove value for specified key
   * @param key Key
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning
   * @return Remove status string
   */

  std::string remove(const key_type &key, bool redirect = false);

  /**
   * @brief Fetch value within the range
   * @param begin_range Begin range
   * @param end_range End range
   * @param redirect Bool value to choose whether to indirect
   * @return
   */

  std::vector<value_type> range_lookup(std::vector<std::string> &data,
                                       const key_type begin_range,
                                       const key_type end_range,
                                       bool redirect = false);

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
   * @brief Update partition name and metadata
   * @param new_name New partition name
   * @param new_metadata New partition metadata
   */

  std::string update_partition(const std::string new_name, const std::string new_metadata);

  /**
   * @brief Fetch storage size
   * @return Storage size
   */
  std::string get_storage_size();

  /**
   * @brief Fetch partition metadata
   * @return Partition metadata
   */

  std::string get_metadata();

  /**
   * @brief Run particular command on key value block
   * @param _return Return status to be collected
   * @param cmd_id Operation identifier
   * @param args Command arguments
   */

  void run_command(std::vector<std::string> &_return, int cmd_id, const std::vector<std::string> &args) override;

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
   * @return Bool value, true if block successfully synchronized
   */

  bool sync(const std::string &path) override;

  /**
   * @brief Flush the block if dirty and clear the block
   * @param path Persistent storage path
   * @return Bool value, true if block successfully dumped
   */

  bool dump(const std::string &path) override;

  /**
   * @brief Send all key and value to the next block
   */

  void forward_all() override;

 private:
  /* Btree map partition */
  btree_type partition_;

  /* Custom serializer/deserializer */
  std::shared_ptr<serde> ser_;

  /* Low threshold */
  double threshold_lo_;
  /* High threshold */
  double threshold_hi_;

  /* Atomic bool for partition hash slot range splitting */
  std::atomic<bool> splitting_;

  /* Atomic bool for partition hash slot range merging */
  std::atomic<bool> merging_;

  /* Atomic partition dirty bit */
  std::atomic<bool> dirty_;

  /* Hash slot range */
  std::pair<int32_t, int32_t> slot_range_;
  /* Bool value for auto scaling */
  std::atomic_bool auto_scale_;
  /* Export slot range */
  std::pair<int32_t, int32_t> export_slot_range_;
  /* Export targets */
  std::vector<std::string> export_target_;
  /* String representation for export target */
  std::string export_target_str_;
  /* Import slot range */
  std::pair<int32_t, int32_t> import_slot_range_;

  /* Directory server hostname */
  std::string directory_host_;

  /* Directory server port number */
  int directory_port_;

};

}
}

#endif //JIFFY_BTREE_SERVICE_SHARD_H
