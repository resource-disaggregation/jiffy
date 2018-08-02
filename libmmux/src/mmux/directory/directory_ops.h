#ifndef MMUX_DIRECTORY_OPS_H
#define MMUX_DIRECTORY_OPS_H

#include <exception>
#include <string>
#include <algorithm>
#include <utility>
#include <vector>
#include <chrono>
#include <sstream>
#include <iostream>

namespace mmux {
namespace directory {

class directory_ops_exception : public std::exception {
 public:
  explicit directory_ops_exception(std::string msg) : msg_(std::move(msg)) {}

  char const *what() const noexcept override {
    return msg_.c_str();
  }
 private:
  std::string msg_;
};

class perms {
 public:
  static const perms none;

  static const perms owner_read;
  static const perms owner_write;
  static const perms owner_exec;
  static const perms owner_all;

  static const perms group_read;
  static const perms group_write;
  static const perms group_exec;
  static const perms group_all;

  static const perms others_read;
  static const perms others_write;
  static const perms others_exec;
  static const perms others_all;

  static const perms all;

  static const perms set_uid;
  static const perms set_gid;
  static const perms sticky_bit;

  static const perms mask;

  perms() : prms_(0) {}

  explicit perms(uint16_t prms) : prms_(prms) {}

  friend inline perms operator&(const perms &p1, const perms &p2) {
    return perms(p1.prms_ & p2.prms_);
  }

  friend inline perms operator|(const perms &p1, const perms &p2) {
    return perms(p1.prms_ | p2.prms_);
  }

  friend inline perms operator^(const perms &p1, const perms &p2) {
    return perms(p1.prms_ ^ p2.prms_);
  }

  perms &operator&=(const perms &p) {
    prms_ &= p.prms_;
    return *this;
  }

  perms &operator|=(const perms &p) {
    prms_ |= p.prms_;
    return *this;
  }

  perms &operator^=(const perms &p) {
    prms_ ^= p.prms_;
    return *this;
  }

  perms operator~() const {
    return perms(~prms_);
  }

  perms &operator=(uint16_t prms) {
    prms_ = prms;
    return *this;
  }

  const uint16_t &operator()() const {
    return prms_;
  }

  bool operator==(const perms &other) const {
    return prms_ == other.prms_;
  }

  bool operator!=(const perms &other) const {
    return prms_ != other.prms_;
  }

  friend std::ostream &operator<<(std::ostream &out, const perms &p) {
    out << ((p & owner_read) != none ? "r" : "-")
        << ((p & owner_write) != none ? "w" : "-")
        << ((p & owner_exec) != none ? "x" : "-")
        << ((p & group_read) != none ? "r" : "-")
        << ((p & group_write) != none ? "w" : "-")
        << ((p & group_exec) != none ? "x" : "-")
        << ((p & others_read) != none ? "r" : "-")
        << ((p & others_write) != none ? "w" : "-")
        << ((p & others_exec) != none ? "x" : "-");
    return out;
  }

 private:
  uint16_t prms_;
};

enum perm_options {
  replace = 0,
  add = 1,
  remove = 2
};

enum file_type {
  none = 0,
  regular = 1,
  directory = 2
};

enum storage_mode {
  in_memory = 0,
  in_memory_grace = 1,
  on_disk = 2
};

enum chain_status {
  stable = 0,
  exporting = 1,
  importing = 2
};

struct replica_chain {
  std::vector<std::string> block_names;
  std::pair<int32_t, int32_t> slot_range;
  chain_status status;
  storage_mode mode;

  replica_chain() : mode(storage_mode::in_memory) {}

  replica_chain(const std::vector<std::string> &block_names,
                int32_t slot_begin,
                int32_t slot_end,
                chain_status status,
                storage_mode mode) {
    this->block_names = block_names;
    this->slot_range.first = slot_begin;
    this->slot_range.second = slot_end;
    this->status = status;
    this->mode = mode;
  }

  replica_chain(const std::vector<std::string> block_names) : mode(storage_mode::in_memory) {
    this->block_names = block_names;
  }

  const std::string slot_range_string() const {
    return std::to_string(slot_range.first) + "_" + std::to_string(slot_range.second);
  }

  const std::string &head() const {
    return block_names.front();
  }

  const std::string &tail() const {
    return block_names.back();
  }

  int32_t slot_begin() const {
    return slot_range.first;
  }

  int32_t slot_end() const {
    return slot_range.second;
  }

  std::string to_string() const {
    std::string out = "<";
    for (const auto &name: block_names) {
      out += name + ", ";
    }
    out.pop_back();
    out.pop_back();
    out += "> :: (" + std::to_string(slot_begin()) + ", " + std::to_string(slot_end()) + ") :: { mode : "
        + std::to_string(mode) + " }";
    return out;
  }

  bool operator==(const replica_chain &other) const {
    return block_names == other.block_names && slot_range == other.slot_range;
  }

  bool operator!=(const replica_chain &other) const {
    return *this != other;
  }
};

class file_status {
 public:
  file_status()
      : type_(file_type::none),
        permissions_(),
        last_write_time_(0) {}

  file_status(file_type type, const perms &prms, std::uint64_t last_write_time)
      : type_(type),
        permissions_(prms),
        last_write_time_(last_write_time) {}

  const file_type &type() const {
    return type_;
  }

  const perms &permissions() const {
    return permissions_;
  }

  void permissions(const perms &prms) {
    permissions_ = prms;
  }

  const std::uint64_t &last_write_time() const {
    return last_write_time_;
  }

  void last_write_time(std::uint64_t time) {
    last_write_time_ = time;
  }

 private:
  file_type type_;
  perms permissions_;
  std::uint64_t last_write_time_;
};

class directory_entry {
 public:
  directory_entry() = default;

  directory_entry(std::string name, const file_status &status)
      : name_(std::move(name)),
        status_(status) {}

  const std::string &name() const {
    return name_;
  }

  const file_type &type() const {
    return status_.type();
  }

  const perms &permissions() const {
    return status_.permissions();
  }

  const std::uint64_t &last_write_time() const {
    return status_.last_write_time();
  }

  const file_status &status() const {
    return status_;
  }

 private:
  std::string name_;
  file_status status_;
};

class data_status {
 public:
  static const std::int32_t PINNED = 0x01;
  static const std::int32_t STATIC_PROVISIONED = 0x02;
  static const std::int32_t MAPPED = 0x04;

  data_status() : chain_length_(1), flags_(0) {}

  data_status(std::string backing_path, std::size_t chain_length, std::vector<replica_chain> blocks, int32_t flags = 0)
      : backing_path_(std::move(backing_path)),
        chain_length_(chain_length),
        data_blocks_(std::move(blocks)),
        flags_(flags) {}

  const std::vector<replica_chain> &data_blocks() const {
    return data_blocks_;
  }

  std::vector<storage_mode> mode() const {
    std::vector<storage_mode> modes(data_blocks_.size());
    for (size_t i = 0; i < data_blocks_.size(); i++) {
      modes[i] = data_blocks_[i].mode;
    }

    return modes;
  }

  void mode(size_t block_id, storage_mode mode) {
    data_blocks_.at(block_id).mode = mode;
  }

  void mode(storage_mode mode) {
    for (size_t i = 0; i < data_blocks_.size(); i++) {
      data_blocks_[i].mode = mode;
    }
  }

  std::vector<std::string> mark_dumped(size_t block_id) {
    data_blocks_.at(block_id).mode = storage_mode::on_disk;
    std::vector<std::string> chain = data_blocks_.at(block_id).block_names;
    data_blocks_.at(block_id).block_names.clear();
    return chain;
  }

  void mark_loaded(size_t block_id, const std::vector<std::string> chain) {
    data_blocks_.at(block_id).mode = storage_mode::in_memory;
    data_blocks_.at(block_id).block_names = chain;
  }

  const std::string &backing_path() const {
    return backing_path_;
  }

  void backing_path(const std::string &backing_path) {
    backing_path_ = backing_path;
  }

  std::size_t chain_length() const {
    return chain_length_;
  }

  void chain_length(std::size_t chain_length) {
    chain_length_ = chain_length;
  }

  void remove_all_data_blocks() {
    data_blocks_.clear();
  }

  std::size_t find_replica_chain(const replica_chain &chain) {
    auto it = std::find(data_blocks_.begin(), data_blocks_.end(), chain);
    if (it == data_blocks_.end()) {
      throw std::logic_error("Could not find replica chain " + chain.to_string());
    }
    return static_cast<size_t>(it - data_blocks_.begin());
  }

  void add_data_block(const replica_chain &block, std::size_t i) {
    data_blocks_.insert(data_blocks_.begin() + i, block);
  }

  void remove_data_block(std::size_t i) {
    data_blocks_.erase(data_blocks_.begin() + i);
  }

  void set_data_block(std::size_t i, replica_chain &&chain) {
    data_blocks_[i] = std::move(chain);
  }

  const replica_chain &get_data_block(std::size_t i) const {
    return data_blocks_[i];
  }

  void update_data_block_slots(std::size_t i, int32_t slot_begin, int32_t slot_end) {
    data_blocks_[i].slot_range.first = slot_begin;
    data_blocks_[i].slot_range.second = slot_end;
  }

  chain_status get_data_block_status(std::size_t i) const {
    return data_blocks_[i].status;
  }

  void set_data_block_status(std::size_t i, chain_status status) {
    data_blocks_[i].status = status;
  }

  int32_t num_slots(std::size_t i) {
    return data_blocks_[i].slot_range.second - data_blocks_[i].slot_range.second;
  }

  std::string to_string() const {
    std::string out = "{ backing_path: " + backing_path_ + ", chain_length: "
        + std::to_string(chain_length_) + ", data_blocks: { ";
    for (const auto &chain: data_blocks_) {
      out += chain.to_string() + ", ";
    }
    out.pop_back();
    out.pop_back();
    out += " }}";
    return out;
  }

  std::int32_t flags() const {
    return flags_;
  }

  void flags(std::int32_t flags) {
    flags_ = flags;
  }

  void set_pinned() {
    flags_ |= PINNED;
  }

  void set_static_provisioned() {
    flags_ |= STATIC_PROVISIONED;
  }

  void set_mapped() {
    flags_ |= MAPPED;
  }

  void clear_flags() {
    flags_ = 0;
  }

  bool is_pinned() const {
    return (flags_ & PINNED) == PINNED;
  }

  bool is_static_provisioned() const {
    return (flags_ & STATIC_PROVISIONED) == STATIC_PROVISIONED;
  }

  bool is_mapped() const {
    return (flags_ & MAPPED) == MAPPED;
  }

 private:
  std::string backing_path_;
  std::size_t chain_length_;
  std::vector<replica_chain> data_blocks_;
  std::int32_t flags_;
};

class directory_ops {
 public:
  virtual ~directory_ops() = default;

  virtual void create_directory(const std::string &path) = 0;
  virtual void create_directories(const std::string &path) = 0;

  virtual data_status open(const std::string &path) = 0;
  virtual data_status create(const std::string &path,
                             const std::string &backing_path,
                             std::size_t num_blocks,
                             std::size_t chain_length,
                             std::int32_t flags) = 0;
  virtual data_status open_or_create(const std::string &path,
                                     const std::string &backing_path,
                                     std::size_t num_blocks,
                                     std::size_t chain_length,
                                     std::int32_t flags) = 0;

  virtual bool exists(const std::string &path) const = 0;

  virtual std::uint64_t last_write_time(const std::string &path) const = 0;

  virtual perms permissions(const std::string &path) = 0;
  virtual void permissions(const std::string &path, const perms &permissions, perm_options opts) = 0;

  virtual void remove(const std::string &path) = 0;
  virtual void remove_all(const std::string &path) = 0;

  virtual void sync(const std::string &path, const std::string &backing_path) = 0;
  virtual void dump(const std::string &path, const std::string &backing_path) = 0;
  virtual void load(const std::string &path, const std::string &backing_path) = 0;

  virtual void rename(const std::string &old_path, const std::string &new_path) = 0;

  virtual file_status status(const std::string &path) const = 0;

  virtual std::vector<directory_entry> directory_entries(const std::string &path) = 0;

  virtual std::vector<directory_entry> recursive_directory_entries(const std::string &path) = 0;

  virtual data_status dstatus(const std::string &path) = 0;

  // Check file type
  virtual bool is_regular_file(const std::string &path) = 0;
  virtual bool is_directory(const std::string &path) = 0;
};

class directory_management_ops {
 public:
  virtual void touch(const std::string &path) = 0;
  virtual replica_chain resolve_failures(const std::string &path, const replica_chain &chain) = 0;
  virtual replica_chain add_replica_to_chain(const std::string &path, const replica_chain &chain) = 0;
  virtual void add_block_to_file(const std::string &path) = 0;
  virtual void split_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end) = 0;
  virtual void merge_slot_range(const std::string &path, int32_t slot_begin, int32_t slot_end) = 0;
  virtual void handle_lease_expiry(const std::string &path) = 0;
};

class directory_interface: public directory_ops, public directory_management_ops {};

}
}

#endif //MMUX_DIRECTORY_OPS_H
