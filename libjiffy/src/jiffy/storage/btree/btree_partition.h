#ifndef JIFFY_BTREE_SERVICE_SHARD_H
#define JIFFY_BTREE_SERVICE_SHARD_H

#include <string>
#include <jiffy/utils/property_map.h>
#include "../serde/serde_all.h"
#include "jiffy/storage/partition.h"
#include "jiffy/persistent/persistent_service.h"
#include "jiffy/storage/chain_module.h"
#include "btree_defs.h"

namespace jiffy {
namespace storage {

class btree_partition : public chain_module {
 public:

  explicit btree_partition(block_memory_manager *manager,
                           const std::string &name = default_name, //TODO need to fix this name
                           const std::string &metadata = "regular", //TODO need to fix this metadata
                           const utils::property_map &conf = {},
                           const std::string &directory_host = "localhost",
                           int directory_port = 9091,
                           const std::string &auto_scaling_host = "localhost",
                           int auto_scaling_port = 9095);

  /**
   * @brief Virtual destructor
   */
  virtual ~btree_partition() = default;

  /**
   * @brief Check if tree contains key
   * @param key Key
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning
   * @return String of key status
   */

  std::string exists(const std::string &key, bool redirect = false);

  /**
   * @brief Put new key value pair
   * @param key Key
   * @param value Value
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning
   * @return Put status string
   */

  std::string put(const std::string &key, const std::string &value, bool redirect = false);

  /**
   * @brief Get value for specified key
   * @param key Key
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning
   * @return Get status string
   */

  std::string get(const std::string &key, bool redirect = false);

  /**
   * @brief Update the value for specified key
   * @param key Key
   * @param value Value
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning
   * @return Update status string
   */

  std::string update(const std::string &key, const std::string &value, bool redirect = false);

  /**
   * @brief Remove value for specified key
   * @param key Key
   * @param redirect Bool value to choose whether to indirect to the destination
   * block when block is in repartitioning
   * @return Remove status string
   */

  std::string remove(const std::string &key, bool redirect = false);

  /**
   * @brief Fetch value within the key range
   * @param data Key value pairs to be fetched
   * @param begin_range Begin range
   * @param end_range End range
   * @param redirect Bool value to choose whether to indirect
   */
  void range_lookup(std::vector<std::string> &data,
                    const std::string &begin_range,
                    const std::string &end_range,
                    bool redirect = false);

  /**
   * @brief Fetch value within the key range
   * @param data Key value pairs to be fetched
   * @param begin_range Begin range
   * @param end_range End range
   * @param batch_size Collect data batch size
   */
  void range_lookup_batches(std::vector<std::string> &data,
                            const std::string &begin_range,
                            const std::string &end_range,
                            size_t batch_size = 1024);

  /**
   * @brief Fetch keys within the key range
   * @param data Keys to be fetched
   * @param begin_range Begin range
   * @param end_range End range
   */
  void range_lookup_keys(std::vector<std::string> &data,
                         const std::string &begin_range,
                         const std::string &end_range);

  /**
   * @brief Counts the key number within the key range
   * @param begin_range Key begin range
   * @param end_range Key end range
   * @param redirect Redirect boolean
   * @return Keys count within the key range
   */
  std::string range_count(const std::string &begin_range,
                          const std::string &end_range,
                          bool redirect = false);

  /**
   * @brief Update partition
   * @param new_name New partition name
   * @param new_metadata New metadata
   * @return Status
   */
  std::string update_partition(const std::string &new_name, const std::string &new_metadata);

  /**
   * @brief Remove partition data during auto scaling
   * @param key Keys to be removed
   * @return Removed value
   */
  std::string scale_remove(const std::string &key);

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

  /**
   * @brief Set block slot range
   * @param slot_begin Begin slot
   * @param slot_end End slot
   */

  void slot_range(const std::string &slot_begin, const std::string &slot_end) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    slot_range_.first = slot_begin;
    slot_range_.second = slot_end;
  }

  /**
   * @brief Fetch slot range
   * @return Block slot range
   */

  const std::pair<std::string, std::string> &slot_range() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return slot_range_;
  }

  /**
   * @brief Check if key is within slot range
   * @param key Key
   * @return Bool value, true if key is in key range
   */
  bool in_slot_range(const std::string &key) {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return key >= slot_range_.first && key < slot_range_.second;
  }

  /**
   * @brief Check if key is within import slot range
   * @param key Key
   * @return Bool value, true if slot is within the range
   */

  bool in_import_slot_range(const std::string &key) {
    return key >= import_slot_range_.first && key < import_slot_range_.second;
  }

  /**
   * @brief Check if key is within export slot range
   * @param key Key
   * @return Bool value, true if slot is within the range
   */

  bool in_export_slot_range(const std::string &key) {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return key >= export_slot_range_.first && key < export_slot_range_.second;
  }

  /**
   * @brief Fetch export target string
   * @return Export target string
   */

  const std::string export_target_str() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return export_target_str_;
  }

  /**
   * @brief Fetch begin slot
   * @return Begin slot
   */

  std::string slot_begin() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return slot_range_.first;
  }

  /**
   * @brief Fetch end slot
   * @return End slot
   */

  std::string slot_end() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return slot_range_.second;
  }

  /**
   * @brief Set the export target
   * @param export_target_string Export target string
   */

  void export_target(const std::string &export_target_string) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    export_target_.clear();
    export_target_ = utils::string_utils::split(export_target_string, '!');
    export_target_str_ = export_target_string;
  }

  /**
   * @brief Set export slot range
   * @param slot_begin Begin slot
   * @param slot_end End slot
   */

  void export_slot_range(std::string slot_begin, std::string slot_end) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    export_slot_range_.first = slot_begin;
    export_slot_range_.second = slot_end;
  }

  /**
   * @brief Set import slot range
   * @param slot_begin Begin slot
   * @param slot_end End slot
   */

  void import_slot_range(std::string slot_begin, std::string slot_end) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    import_slot_range_.first = slot_begin;
    import_slot_range_.second = slot_end;
  }

  /**
   * @brief Fetch import slot range
   * @return Import slot range
   */

  const std::pair<std::string, std::string> &import_slot_range() {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return import_slot_range_;
  };

  /**
   * @brief Set slot range based on the partition name
   * @param new_name New partition name
   */
  void slot_range(const std::string &new_name) {
    auto slots = utils::string_utils::split(new_name, '_', 2);
    slot_range_.first = slots[0];
    slot_range_.second = slots[1];
  }

 private:

  /**
   * @brief Check if block is overloaded
   * @return Bool value, true if block size is over the high threshold capacity
   */

  bool overload();

  /**
   * @brief Check if block is underloaded
   * @return Bool value, true if block size is under the low threshold capacity
   */

  bool underload();

  /* Btree map partition */
  btree_type partition_;

  /* Custom serializer/deserializer */
  std::shared_ptr<serde> ser_;

  /* Low threshold */
  double threshold_lo_;
  /* High threshold */
  double threshold_hi_;

  /* Atomic bool for partition slot range splitting */
  std::atomic<bool> splitting_;

  /* Atomic bool for partition slot range merging */
  std::atomic<bool> merging_;

  /* Atomic partition dirty bit */
  std::atomic<bool> dirty_;

  /* Slot range */
  std::pair<std::string, std::string> slot_range_;
  /* Bool value for auto scaling */
  std::atomic_bool auto_scale_;
  /* Export slot range */
  std::pair<std::string, std::string> export_slot_range_;
  /* Export targets */
  std::vector<std::string> export_target_;
  /* String representation for export target */
  std::string export_target_str_;
  /* Import slot range */
  std::pair<std::string, std::string> import_slot_range_;

  /* Directory server hostname */
  std::string directory_host_;

  /* Directory server port number */
  int directory_port_;

  /* Auto scaling server host name */
  std::string auto_scaling_host_;

  /* Auto scaling server port number */
  int auto_scaling_port_;

  /* Data update mutex */
  std::mutex update_lock;
};

}
}

#endif //JIFFY_BTREE_SERVICE_SHARD_H
