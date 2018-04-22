#ifndef ELASTICMEM_DIRECTORY_SERVICE_H
#define ELASTICMEM_DIRECTORY_SERVICE_H

#include <exception>
#include <string>
#include <algorithm>
#include <utility>
#include <vector>
#include <chrono>
#include <sstream>

namespace elasticmem {
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
  flushing = 2,
  on_disk = 3
};

enum chain_status {
  stable = 0,
  exporting = 1,
};

struct replica_chain {
  std::vector<std::string> block_names;
  std::pair<int32_t, int32_t> slot_range;
  chain_status status;

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
    out += ">";
    return out;
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

 private:
  std::string name_;
  file_status status_;
};

class data_status {
 public:
  data_status() : mode_(storage_mode::in_memory), chain_length_(1) {}

  data_status(storage_mode mode,
              std::string persistent_store_prefix,
              std::size_t chain_length,
              std::vector<replica_chain> blocks)
      : mode_(mode),
        persistent_store_prefix_(std::move(persistent_store_prefix)),
        chain_length_(chain_length),
        data_blocks_(std::move(blocks)) {}

  const std::vector<replica_chain> &data_blocks() const {
    return data_blocks_;
  }

  const storage_mode &mode() const {
    return mode_;
  }

  void mode(storage_mode mode) {
    mode_ = mode;
  }

  const std::string &persistent_store_prefix() const {
    return persistent_store_prefix_;
  }

  void persistent_store_prefix(const std::string &prefix) {
    persistent_store_prefix_ = prefix;
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

  void add_data_block(const replica_chain &block, std::size_t i) {
    data_blocks_.insert(data_blocks_.begin() + i, block);
  }

  void remove_data_block(std::size_t i) {
    data_blocks_.erase(data_blocks_.begin() + i);
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

 private:
  storage_mode mode_;
  std::string persistent_store_prefix_;
  std::size_t chain_length_;
  std::vector<replica_chain> data_blocks_;
};

class directory_ops {
 public:
  virtual ~directory_ops() = default;

  virtual void create_directory(const std::string &path) = 0;
  virtual void create_directories(const std::string &path) = 0;

  virtual data_status open(const std::string &path) = 0;
  virtual data_status create(const std::string &path,
                             const std::string &persistent_store_prefix,
                             std::size_t num_blocks,
                             std::size_t chain_length) = 0;
  virtual data_status open_or_create(const std::string &path,
                                     const std::string &persistent_store_prefix,
                                     std::size_t num_blocks,
                                     std::size_t chain_length) = 0;

  virtual bool exists(const std::string &path) const = 0;

  virtual std::uint64_t last_write_time(const std::string &path) const = 0;

  virtual perms permissions(const std::string &path) = 0;
  virtual void permissions(const std::string &path, const perms &permsissions, perm_options opts) = 0;

  virtual void remove(const std::string &path) = 0;
  virtual void remove_all(const std::string &path) = 0;

  virtual void flush(const std::string &path) = 0;

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
  virtual void split_block(const std::string &path, std::size_t block_idx) = 0;
};

}
}

#endif //ELASTICMEM_DIRECTORY_SERVICE_H
