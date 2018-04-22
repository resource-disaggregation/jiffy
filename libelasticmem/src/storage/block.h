#ifndef ELASTICMEM_BLOCK_H
#define ELASTICMEM_BLOCK_H

#include <utility>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <stdexcept>
#include <iostream>
#include "notification/subscription_map.h"
#include "service/block_response_client_map.h"

#define MAX_BLOCK_OP_NAME_SIZE 63

namespace elasticmem {
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
        export_slot_range_(0, -1) {}

  virtual void run_command(std::vector<std::string> &_return, int32_t oid, const std::vector<std::string> &args) = 0;

  void run_command(std::vector<std::string> &_return,
                   const std::string &op_name,
                   const std::vector<std::string> &args) {
    int32_t oid = op_id(op_name);
    if (oid == -1)
      throw std::invalid_argument("No such command " + op_name);
    run_command(_return, oid, args);
  }

  void path(const std::string &path) {
    path_ = path;
  }

  const std::string &path() const {
    return path_;
  }

  const std::string &name() const {
    return block_name_;
  }

  void slot_range(int32_t slot_begin, int32_t slot_end) {
    slot_range_.first = slot_begin;
    slot_range_.second = slot_end;
  }

  const std::pair<int32_t, int32_t> &slot_range() const {
    return slot_range_;
  }

  int32_t slot_begin() const {
    return slot_range_.first;
  }

  int32_t slot_end() const {
    return slot_range_.second;
  }

  bool in_slot_range(int32_t slot) {
    return slot >= slot_range_.first && slot <= slot_range_.second;
  }

  void state(block_state state) {
    state_ = state;
  }

  const block_state &state() const {
    return state_;
  }

  void export_slot_range(int32_t slot_begin, int32_t slot_end) {
    export_slot_range_.first = slot_begin;
    export_slot_range_.second = slot_end;
  }

  const std::pair<int32_t, int32_t>& export_slot_range() {
    return export_slot_range_;
  };

  bool in_export_slot_range(int32_t slot) {
    return slot >= export_slot_range_.first && slot <= export_slot_range_.second;
  }

  void export_target(const std::vector<std::string>& target) {
    export_target_ = target;
    for (const auto& block: target) {
      export_target_str_ += (block + "!");
    }
    export_target_str_.pop_back();
  }

  const std::vector<std::string>& export_target() const {
    return export_target_;
  }

  const std::string export_target_str() const {
    return export_target_str_;
  }

  bool is_accessor(int i) const {
    return block_ops_.at(static_cast<size_t>(i)).type == accessor;
  }

  bool is_mutator(int i) const {
    return block_ops_.at(static_cast<size_t>(i)).type == mutator;
  }

  int op_id(const std::string &op_name) const {
    return search_op_name(0, static_cast<int>(block_ops_.size() - 1), op_name.c_str());
  }

  std::string op_name(int op_id) {
    return block_ops_[op_id].name;
  }

  /** Management Operations **/
  virtual void load(const std::string &remote_storage_prefix, const std::string &path) = 0;

  virtual void flush(const std::string &remote_storage_prefix, const std::string &path) = 0;

  virtual std::size_t storage_capacity() = 0;

  virtual std::size_t storage_size() = 0;

  virtual void reset() = 0;

  virtual void export_slots() = 0;

  subscription_map &subscriptions() {
    return sub_map_;
  }

  block_response_client_map &clients() {
    return client_map_;
  }

 private:
  int32_t search_op_name(int32_t l, int32_t r, const char *name) const {
    if (r >= l) {
      int32_t mid = l + (r - l) / 2;
      if (std::strcmp(block_ops_[mid].name, name) == 0)
        return mid;
      if (std::strcmp(block_ops_[mid].name, name) > 0)
        return search_op_name(l, mid - 1, name);
      return search_op_name(mid + 1, r, name);
    }
    return -1;
  }

  const std::vector<block_op> &block_ops_;
  std::string path_;
  std::string block_name_;

  block_state state_;
  std::pair<int32_t, int32_t> slot_range_;

  std::pair<int32_t, int32_t> export_slot_range_;
  std::vector<std::string> export_target_;
  std::string export_target_str_;

  subscription_map sub_map_{};
  block_response_client_map client_map_{};
};

}
}
#endif //ELASTICMEM_BLOCK_H
