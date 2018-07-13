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

enum block_op_type : uint8_t {
  accessor = 0,
  mutator = 1
};

enum block_state {
  regular = 0,
  importing = 1,
  exporting = 2
};

struct block_op {
  block_op_type type;
  char name[MAX_BLOCK_OP_NAME_SIZE];

  bool operator<(const block_op &other) const {
    return std::strcmp(name, other.name) < 0;
  }
};

// TODO: Setting metadata should be atomic: e.g., reset function, or setup block function, should acquire lock before
// setting metadata
class block {
 public:
  static const int32_t SLOT_MAX = 65536;

  explicit block(const std::vector<block_op> &block_ops, std::string block_name)
      : block_ops_(block_ops),
        path_(""),
        block_name_(std::move(block_name)),
        state_(block_state::regular),
        slot_range_(0, -1),
        auto_scale_(true),
        export_slot_range_(0, -1),
        import_slot_range_(0, -1) {}

  virtual void run_command(std::vector<std::string> &_return, int32_t oid, const std::vector<std::string> &args) = 0;

  void path(const std::string &path) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    path_ = path;
  }

  const std::string &path() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return path_;
  }

  const std::string &name() const {
    return block_name_; // Does not require locking since block_name does not change
  }

  void slot_range(int32_t slot_begin, int32_t slot_end) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    slot_range_.first = slot_begin;
    slot_range_.second = slot_end;
  }

  const std::pair<int32_t, int32_t> &slot_range() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return slot_range_;
  }

  int32_t slot_begin() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return slot_range_.first;
  }

  int32_t slot_end() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return slot_range_.second;
  }

  bool in_slot_range(int32_t slot) {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return slot >= slot_range_.first && slot <= slot_range_.second;
  }

  void state(block_state state) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    state_ = state;
  }

  const block_state &state() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return state_;
  }

  void export_slot_range(int32_t slot_begin, int32_t slot_end) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    export_slot_range_.first = slot_begin;
    export_slot_range_.second = slot_end;
  }

  const std::pair<int32_t, int32_t> &export_slot_range() {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return export_slot_range_;
  };

  bool in_export_slot_range(int32_t slot) {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return slot >= export_slot_range_.first && slot <= export_slot_range_.second;
  }

  void import_slot_range(int32_t slot_begin, int32_t slot_end) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    import_slot_range_.first = slot_begin;
    import_slot_range_.second = slot_end;
  }

  const std::pair<int32_t, int32_t> &import_slot_range() {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return import_slot_range_;
  };

  bool in_import_slot_range(int32_t slot) {
    return slot >= import_slot_range_.first && slot <= import_slot_range_.second;
  }

  void export_target(const std::vector<std::string> &target) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    export_target_ = target;
    export_target_str_ = "";
    for (const auto &block: target) {
      export_target_str_ += (block + "!");
    }
    export_target_str_.pop_back();
  }

  const std::vector<std::string> &export_target() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return export_target_;
  }

  const std::string export_target_str() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return export_target_str_;
  }

  bool is_accessor(int i) const {
    return block_ops_.at(static_cast<size_t>(i)).type == accessor; // Does not require lock since block_ops don't change
  }

  bool is_mutator(int i) const {
    return block_ops_.at(static_cast<size_t>(i)).type == mutator; // Does not require lock since block_ops don't change
  }

  std::string op_name(int op_id) {
    return block_ops_[op_id].name;  // Does not require lock since block_ops don't change
  }

  /** Management Operations **/
  virtual void load(const std::string &path) = 0;

  virtual bool sync(const std::string &path) = 0;

  virtual bool dump(const std::string &path) = 0;

  virtual std::size_t storage_capacity() = 0;

  virtual std::size_t storage_size() = 0;

  virtual void reset() = 0;

  virtual void export_slots() = 0;

  subscription_map &subscriptions() {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return sub_map_;
  }

  block_response_client_map &clients() {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return client_map_;
  }

 protected:
  mutable std::shared_mutex metadata_mtx_;

  const std::vector<block_op> &block_ops_;
  std::string path_;
  std::string block_name_;

  block_state state_;
  std::pair<int32_t, int32_t> slot_range_;
  std::atomic_bool auto_scale_;

  std::pair<int32_t, int32_t> export_slot_range_;
  std::vector<std::string> export_target_;
  std::string export_target_str_;

  std::pair<int32_t, int32_t> import_slot_range_;

  subscription_map sub_map_{};
  block_response_client_map client_map_{};
};

}
}
#endif //MMUX_BLOCK_H
