#ifndef JIFFY_KV_SERVICE_SHARD_H
#define JIFFY_KV_SERVICE_SHARD_H

#include <string>
#include <jiffy/utils/property_map.h>
#include "jiffy/storage/serde/serde_all.h"
#include "jiffy/storage/partition.h"
#include "jiffy/persistent/persistent_service.h"
#include "jiffy/storage/chain_module.h"
#include "jiffy/storage/hashtable/hash_table_ops.h"
#include "hash_table_defs.h"

namespace jiffy {
namespace storage {

#define BEGIN_CATCH_HANDLER                       \
  try {                                           \
  auto it = block_.find(make_binary(args[1]))



#define END_CATCH_HANDLER                         \
  } catch (std::bad_alloc &e) {                   \
  RETURN_ERR("!redo");                            \
}

/* Key value partition structure class, inherited from chain module */
class hash_table_partition : public chain_module {
 public:

  /**
   * @brief Constructor
   * @param conf Configuration properties
   */
  explicit hash_table_partition(block_memory_manager *manager,
                                const std::string &name = "0_65536",
                                const std::string &metadata = "regular",
                                const utils::property_map &conf = {},
                                const std::string &auto_scaling_host = "localhost",
                                int auto_scaling_port = 9095);

  /**
   * @brief Virtual destructor
   */
  ~hash_table_partition() override = default;

  /**
   * @brief Set block hash slot range
   * @param slot_begin Begin slot
   * @param slot_end End slot
   */
  void slot_range(int32_t slot_begin, int32_t slot_end) {
    slot_range_.first = slot_begin;
    slot_range_.second = slot_end;
  }

  /**
   * @brief Set slot range based on the partition name
   * @param new_name New partition name
   */
  void slot_range(const std::string &new_name) {
    auto slots = utils::string_utils::split(new_name, '_', 2);
    slot_range_.first = std::stoi(slots[0]);
    slot_range_.second = std::stoi(slots[1]);
  }

  /**
   * @brief Fetch slot range
   * @return Block slot range
   */
  const std::pair<int32_t, int32_t> &slot_range() const {
    return slot_range_;
  }

  /**
   * @brief Fetch begin slot
   * @return Begin slot
   */
  int32_t slot_begin() const {
    return slot_range_.first;
  }

  /**
   * @brief Fetch end slot
   * @return End slot
   */
  int32_t slot_end() const {
    return slot_range_.second;
  }

  /**
   * @brief Check if slot is within the slot range
   * @param slot Slot
   * @return Bool value, true if slot is within the range
   */
  bool in_slot_range(int32_t slot) {
    return slot >= slot_range_.first && slot < slot_range_.second;
  }

  /**
   * @brief Set export slot range
   * @param slot_begin Begin slot
   * @param slot_end End slot
   */
  void export_slot_range(int32_t slot_begin, int32_t slot_end) {
    export_slot_range_.first = slot_begin;
    export_slot_range_.second = slot_end;
  };

  /**
   * @brief Check if slot is within export slot range
   * @param slot Slot
   * @return Bool value, true if slot is within the range
   */
  bool in_export_slot_range(int32_t slot) {
    return slot >= export_slot_range_.first && slot < export_slot_range_.second;
  }

  /**
   * @brief Set import slot range
   * @param slot_begin Begin slot
   * @param slot_end End slot
   */
  void import_slot_range(int32_t slot_begin, int32_t slot_end) {
    import_slot_range_.first = slot_begin;
    import_slot_range_.second = slot_end;
  }

  /**
   * @brief Fetch import slot range
   * @return Import slot range
   */
  const std::pair<int32_t, int32_t> &import_slot_range() {
    return import_slot_range_;
  };

  /**
   * @brief Check if slot is within import slot range
   * @param slot Slot
   * @return Bool value, true if slot is within the range
   */
  bool in_import_slot_range(int32_t slot) {
    return slot >= import_slot_range_.first && slot < import_slot_range_.second;
  }

  /**
   * @brief Set the export target
   * @param export_target_string Export target string
   */
  void export_target(const std::string &export_target_string) {
    export_target_.clear();
    export_target_ = utils::string_utils::split(export_target_string, '!');
    export_target_str_ = export_target_string;
  }

  /**
   * @brief Check if hash map contains key
   * @param _return Response
   * @param args Arguments
   */
  void exists(response &_return, const arg_list &args);

  /**
   * @brief Insert new key value pair
   * @param _return Response
   * @param args Arguments
   */
  void put(response &_return, const arg_list &args);

  /**
   * @brief Insert or update key value pair
   * @param _return Response
   * @param args Arguments
   */
  void upsert(response &_return, const arg_list &args);

  /**
   * @brief Get value for specified key
   * @param _return Response
   * @param args Arguments
   */
  void get(response &_return, const arg_list &args);

  /**
   * @brief Update the value for specified key
   * @param _return Response
   * @param args Arguments
   */
  void update(response &_return, const arg_list &args);

  /**
   * @brief Remove value for specified key
   * @param _return Response
   * @param args Arguments
   */
  void remove(response &_return, const arg_list &args);

  /**
   * @brief Remove key from hash table during scaling
   * @param _return Response
   * @param args Arguments
   */
  void scale_remove(response &_return, const arg_list &args);

  /**
   * @brief Insert key value pair to hash table during scaling
   * @param _return Response
   * @param args Arguments
   */
  void scale_put(response &_return, const arg_list &args);

  /**
   * @brief Fetch data from keys which lie in slot range
   * @param data Data to be fetched
   * @param _return Response
   * @param args Arguments
   */
  void get_data_in_slot_range(response &_return, const arg_list &args);

  /**
   * @brief Update partition name and metadata
   * @param _return Response
   * @param args Arguments
   */
  void update_partition(response &_return, const arg_list &args);

  /**
   * @brief Fetch storage size
   * @param _return Response
   * @param args Arguments
   */
  void get_storage_size(response &_return, const arg_list &args);

  /**
   * @brief Fetch partition metadata
   * @param _return Response
   * @param args Arguments
   */
  void get_metadata(response &_return, const arg_list &args);

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
   * @param _return Response
   * @param args Arguments
   */
  void run_command_impl(response &_return, const arg_list &args) override;

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

  /**
   * @brief Remove all keys in the remove buffer
   */
  void buffer_remove();

  /**
   * @brief Construct binary string for temporary values
   * @param str String
   * @return Binary string
   */
  binary make_temporary_binary(const std::string &str) {
    return binary(str, temporary_data_allocator_);
  }

  /* Cuckoo hash map partition */
  std::unordered_map<key_type, value_type, hash_type, equal_type> block_;

  /* Custom serializer/deserializer */
  std::shared_ptr<serde> ser_;

  /* Low threshold */
  double threshold_lo_;

  /* High threshold */
  double threshold_hi_;

  /* Bool for partition hash slot range splitting */
  bool scaling_up_;

  /* Bool for partition hash slot range merging */
  bool scaling_down_;

  /* Bool partition dirty bit */
  bool dirty_;

  /* Hash slot range */
  std::pair<int32_t, int32_t> slot_range_;

  /* Bool value for auto scaling */
  bool auto_scale_;

  /* Export slot range */
  std::pair<int32_t, int32_t> export_slot_range_;

  /* Export targets */
  std::vector<std::string> export_target_;

  /* String representation for export target */
  std::string export_target_str_;

  /* Import slot range */
  std::pair<int32_t, int32_t> import_slot_range_;

  /* Auto scaling server hostname */
  std::string auto_scaling_host_;

  /* Auto scaling server port number */
  int auto_scaling_port_;

  /* Data update mutex, we want only one update function happen at a time */
  std::mutex update_lock_;

  /* Buffer remove cache */
  std::map<std::string, int> remove_cache_;

  /* Temporary data allocator */
  allocator<uint8_t> temporary_data_allocator_;

  /* Temporary data manager */
  block_memory_manager* temporary_data_manager_;

};

}
}

#endif //JIFFY_KV_SERVICE_SHARD_H
