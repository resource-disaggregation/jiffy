#ifndef MMUX_BLOCK_H
#define MMUX_BLOCK_H

#include <utility>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <stdexcept>
#include <iostream>
#include <shared_mutex>
#include "notification/subscription_map.h"
#include "service/block_response_client_map.h"

#define MAX_BLOCK_OP_NAME_SIZE 63

namespace mmux {
namespace storage {

/*
 * Block operation type
 * Mutator can be read and written
 * Accessor can only be read
 */

enum block_op_type : uint8_t {
  accessor = 0,
  mutator = 1
};

/*
 * Block state
 */

enum block_state {
  regular = 0,
  importing = 1,
  exporting = 2
};

/*
 * Block operation structure
 */

struct block_op {
  block_op_type type;
  char name[MAX_BLOCK_OP_NAME_SIZE];

  /**
   * @brief Operator < to check if name is smaller in Lexicographical order
   * @param other Other block operation
   * @return Bool value
   */

  bool operator<(const block_op &other) const {
    return std::strcmp(name, other.name) < 0;
  }
};

// TODO: Setting metadata should be atomic: e.g., reset function, or setup block function, should acquire lock before
// setting metadata
/* Block class */
class block {
 public:
  /* Slot range max */
  static const int32_t SLOT_MAX = 65536;

  /**
   * @brief Constructor
   * @param block_ops Block operations
   * @param block_name Block name
   */

  explicit block(const std::vector<block_op> &block_ops, std::string block_name)
      : block_ops_(block_ops),
        path_(""),
        block_name_(std::move(block_name)),
        state_(block_state::regular),
        slot_range_(0, -1),
        auto_scale_(true),
        export_slot_range_(0, -1),
        import_slot_range_(0, -1) {}

  /**
   * @brief Virtual function for running a command on a block
   * @param _return Return value
   * @param oid Operation identifier
   * @param args Operation arguments
   */

  virtual void run_command(std::vector<std::string> &_return, int32_t oid, const std::vector<std::string> &args) = 0;

  /**
   * @brief Set block path
   * @param path Block path
   */

  void path(const std::string &path) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    path_ = path;
  }

  /**
   * @brief Fetch block path
   * @return Block path
   */

  const std::string &path() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return path_;
  }

  /**
   * @brief Fetch block name
   * @return Block name
   */

  const std::string &name() const {
    return block_name_; // Does not require locking since block_name does not change
  }

  /**
   * @brief Set block hash slot range
   * @param slot_begin Begin slot
   * @param slot_end End slot
   */

  void slot_range(int32_t slot_begin, int32_t slot_end) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    slot_range_.first = slot_begin;
    slot_range_.second = slot_end;
  }

  /**
   * @brief Fetch slot range
   * @return Block slot range
   */

  const std::pair<int32_t, int32_t> &slot_range() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return slot_range_;
  }

  /**
   * @brief Fetch begin slot
   * @return Begin slot
   */

  int32_t slot_begin() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return slot_range_.first;
  }

  /**
   * @brief Fetch end slot
   * @return End slot
   */

  int32_t slot_end() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return slot_range_.second;
  }

  /**
   * @brief Check if slot is within the slot range
   * @param slot Slot
   * @return Bool value, true if slot is within the range
   */

  bool in_slot_range(int32_t slot) {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return slot >= slot_range_.first && slot <= slot_range_.second;
  }

  /**
   * @brief Set block state
   * @param state Block state
   */

  void state(block_state state) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    state_ = state;
  }

  /**
   * @brief Fetch block state
   * @return Block state
   */

  const block_state &state() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return state_;
  }

  /**
   * @brief Set export slot range
   * @param slot_begin Begin slot
   * @param slot_end End slot
   */

  void export_slot_range(int32_t slot_begin, int32_t slot_end) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    export_slot_range_.first = slot_begin;
    export_slot_range_.second = slot_end;
  }

  /**
   * @brief Fetch export slot range
   * @return Export slot range
   */

  const std::pair<int32_t, int32_t> &export_slot_range() {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return export_slot_range_;
  };

  /**
   * @brief Check if slot is within export slot range
   * @param slot Slot
   * @return Bool value, true if slot is within the range
   */

  bool in_export_slot_range(int32_t slot) {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return slot >= export_slot_range_.first && slot <= export_slot_range_.second;
  }

  /**
   * @brief Set import slot range
   * @param slot_begin Begin slot
   * @param slot_end End slot
   */

  void import_slot_range(int32_t slot_begin, int32_t slot_end) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    import_slot_range_.first = slot_begin;
    import_slot_range_.second = slot_end;
  }

  /**
   * @brief Fetch import slot range
   * @return Import slot range
   */

  const std::pair<int32_t, int32_t> &import_slot_range() {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return import_slot_range_;
  };

  /**
   * @brief Check if slot is within import slot range
   * @param slot Slot
   * @return Bool value, true if slot is within the range
   */

  bool in_import_slot_range(int32_t slot) {
    return slot >= import_slot_range_.first && slot <= import_slot_range_.second;
  }

  /**
   * @brief Set the export target
   * @param target Export target
   */

  void export_target(const std::vector<std::string> &target) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    export_target_ = target;
    export_target_str_ = "";
    for (const auto &block: target) {
      export_target_str_ += (block + "!");
    }
    export_target_str_.pop_back();
  }

  /**
   * @brief Fetch export target
   * @return Export target
   */

  const std::vector<std::string> &export_target() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return export_target_;
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
   * @brief Check if ith block operation type is accessor
   * @param i Block operation identifier
   * @return Bool value, true if block is accessor
   */

  bool is_accessor(int i) const {
    return block_ops_.at(static_cast<size_t>(i)).type == accessor; // Does not require lock since block_ops don't change
  }

  /**
   * @brief Check if ith block operation type is mutator
   * @param i Block operation identifier
   * @return Bool value, true if is mutator
   */

  bool is_mutator(int i) const {
    return block_ops_.at(static_cast<size_t>(i)).type == mutator; // Does not require lock since block_ops don't change
  }

  /**
   * @brief Fetch operation name
   * @param op_id Operation identifier
   * @return Operation name
   */

  std::string op_name(int op_id) {
    return block_ops_[op_id].name;  // Does not require lock since block_ops don't change
  }

  /**
   * Management Operations
   * Virtual function
   */

  virtual void load(const std::string &path) = 0;

  virtual bool sync(const std::string &path) = 0;

  virtual bool dump(const std::string &path) = 0;

  virtual std::size_t storage_capacity() = 0;

  virtual std::size_t storage_size() = 0;

  virtual void reset() = 0;

  virtual void export_slots() = 0;

  /**
   * @brief Fetch subscription map
   * @return Subscription map
   */

  subscription_map &subscriptions() {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return sub_map_;
  }

  /**
   * @brief Fetch block response client map
   * @return Block response client map
   */

  block_response_client_map &clients() {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return client_map_;
  }

 protected:
  /* Metadata mutex */
  mutable std::shared_mutex metadata_mtx_;
  /* Block operations */
  const std::vector<block_op> &block_ops_;
  /* Block file path */
  std::string path_;
  /* Block name */
  std::string block_name_;
  /* Block state, regular, importing or exporting */
  block_state state_;
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
  /* Subscription map */
  subscription_map sub_map_{};
  /* Block response client map */
  block_response_client_map client_map_{};
};

}
}
#endif //MMUX_BLOCK_H