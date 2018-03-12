#ifndef ELASTICMEM_DIRECTORY_SERVICE_H
#define ELASTICMEM_DIRECTORY_SERVICE_H

#include <exception>
#include <string>
#include <algorithm>
#include <utility>
#include <vector>
#include <chrono>

namespace elasticmem {
namespace directory {

class directory_service_exception : public std::exception {
 public:
  explicit directory_service_exception(std::string msg) : msg_(std::move(msg)) {}

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
  data_status() : mode_(storage_mode::in_memory) {}
  data_status(storage_mode mode, std::string persistent_store_prefix, std::vector<std::string> nodes)
      : mode_(mode), persistent_store_prefix_(std::move(persistent_store_prefix)), data_blocks_(std::move(nodes)) {}

  const std::vector<std::string> &data_blocks() const {
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

  void persistent_store_prefix(const std::string& prefix) {
    persistent_store_prefix_ = prefix;
  }

  void remove_all_data_blocks() {
    data_blocks_.clear();
  }

  void add_data_block(const std::string &block) {
    data_blocks_.push_back(block);
  }

  void remove_data_block(const std::string &block) {
    data_blocks_.erase(std::remove(data_blocks_.begin(), data_blocks_.end(), block), data_blocks_.end());
  }

 private:
  storage_mode mode_;
  std::string persistent_store_prefix_;
  std::vector<std::string> data_blocks_;
};

class directory_service {
 public:
  virtual ~directory_service() = default;

  virtual void create_directory(const std::string &path) = 0;
  virtual void create_directories(const std::string &path) = 0;

  virtual void create_file(const std::string &path) = 0;

  virtual bool exists(const std::string &path) const = 0;

  virtual std::size_t file_size(const std::string &path) const = 0;

  virtual std::uint64_t last_write_time(const std::string &path) const = 0;

  virtual perms permissions(const std::string &path) = 0;
  virtual void permissions(const std::string &path, const perms &permsissions, perm_options opts) = 0;

  virtual void remove(const std::string &path) = 0;
  virtual void remove_all(const std::string &path) = 0;

  virtual void rename(const std::string &old_path, const std::string &new_path) = 0;

  virtual file_status status(const std::string &path) const = 0;

  virtual std::vector<directory_entry> directory_entries(const std::string &path) = 0;

  virtual std::vector<directory_entry> recursive_directory_entries(const std::string &path) = 0;

  virtual data_status dstatus(const std::string &path) = 0;

  virtual storage_mode mode(const std::string &path) = 0;

  virtual std::string persistent_store_prefix(const std::string &path) = 0;

  virtual std::vector<std::string> data_blocks(const std::string &path) = 0;

  // Check file type
  virtual bool is_regular_file(const std::string &path) = 0;
  virtual bool is_directory(const std::string &path) = 0;
};

class directory_management_service {
 public:
  virtual void touch(const std::string &path) = 0;

  virtual void grow(const std::string &path, std::size_t bytes) = 0;

  virtual void shrink(const std::string &path, std::size_t bytes) = 0;

  virtual void dstatus(const std::string &path, const data_status &status) = 0;

  virtual void mode(const std::string &path, const storage_mode &mode) = 0;

  virtual void persistent_store_prefix(const std::string& path, const std::string& prefix) = 0;

  virtual void add_data_block(const std::string &path) = 0;

  virtual void remove_data_block(const std::string &path, const std::string &node) = 0;

  virtual void remove_all_data_blocks(const std::string &path) = 0;
};

}
}

#endif //ELASTICMEM_DIRECTORY_SERVICE_H
