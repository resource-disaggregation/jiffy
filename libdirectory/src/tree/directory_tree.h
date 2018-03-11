#ifndef ELASTICMEM_DIRECTORY_SERVICE_SHARD_H
#define ELASTICMEM_DIRECTORY_SERVICE_SHARD_H

#include <utility>
#include <memory>
#include <atomic>
#include <map>
#include <shared_mutex>

#include "../directory_service.h"

namespace elasticmem {
namespace directory {

class ds_node {
 public:
  explicit ds_node(const std::string &name, file_status status)
      : name_(std::move(name)), status_(status) {}

  virtual ~ds_node() = default;

  const std::string &name() const { return name_; }

  void name(const std::string &name) { name_ = name; }

  bool is_directory() const { return status_.type() == file_type::directory; }

  bool is_regular_file() const { return status_.type() == file_type::regular; }

  file_status status() const { return status_; }

  directory_entry entry() const { return directory_entry(name_, status_); }

  virtual std::size_t file_size() const = 0;

  std::time_t last_write_time() const { return status_.last_write_time(); }

  void permissions(const perms &prms) { status_.permissions(prms); }

  perms permissions() const { return status_.permissions(); }

  void last_write_time(std::time_t time) { status_.last_write_time(time); }

 private:
  std::string name_{};
  file_status status_{};
};

class ds_dir_node : public ds_node {
 public:
  typedef std::map<std::string, std::shared_ptr<ds_node>> child_map;

  explicit ds_dir_node(const std::string &name)
      : ds_node(name, file_status(file_type::directory, perms(perms::all), std::time(nullptr))) {}

  std::shared_ptr<ds_node> get_child(const std::string &name) const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    auto ret = children_.find(name);
    if (ret != children_.end()) {
      return ret->second;
    } else {
      return nullptr;
    }
  }

  void add_child(std::shared_ptr<ds_node> node) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    if (children_.find(node->name()) == children_.end()) {
      children_.insert(std::make_pair(node->name(), node));
    } else {
      throw directory_service_exception("Child node already exists: " + node->name());
    }
  }

  void remove_child(const std::string &name) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    auto ret = children_.find(name);
    if (ret != children_.end()) {
      children_.erase(ret);
    } else {
      throw directory_service_exception("Child node not found: " + name);
    }
  }

  std::vector<directory_entry> entries() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    std::vector<directory_entry> ret;
    ret.reserve(children_.size());
    populate_entries(ret);
    return ret;
  }

  std::vector<directory_entry> recursive_entries() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    std::vector<directory_entry> ret;
    populate_recursive_entries(ret);
    return ret;
  }

  std::size_t file_size() const override {
    std::size_t size = 0;
    for (const auto &entry: children_) {
      size += entry.second->file_size();
    }
    return size;
  }

  child_map::const_iterator begin() const {
    return children_.begin();
  }

  child_map::const_iterator end() const {
    return children_.end();
  }

  std::size_t size() const {
    return children_.size();
  }

  bool empty() const {
    return children_.empty();
  }

 private:
  void populate_entries(std::vector<directory_entry> &entries) const {
    for (auto &entry: children_) {
      entries.emplace_back(entry.second->entry());
    }
  }

  void populate_recursive_entries(std::vector<directory_entry> &entries) const {
    for (auto &entry: children_) {
      entries.emplace_back(entry.second->entry());
      if (entry.second->is_directory()) {
        std::dynamic_pointer_cast<ds_dir_node>(entry.second)->populate_recursive_entries(entries);
      }
    }
  }

  mutable std::shared_mutex mtx_;
  child_map children_{};
};

class ds_file_node : public ds_node {
 public:
  explicit ds_file_node(const std::string &name)
      : ds_node(name, file_status(file_type::regular, perms(perms::all), std::time(nullptr))), dstatus_{}, size_(0) {}

  const data_status &dstatus() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_;
  }

  void dstatus(const data_status &status) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_ = status;
  }

  const storage_mode &mode() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.mode();
  }

  void mode(const storage_mode &m) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.mode(m);
  }

  const std::string &persistent_store_prefix() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.persistent_store_prefix();
  }

  void persistent_store_prefix(const std::string& prefix) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.persistent_store_prefix(prefix);
  }

  const std::vector<std::string> &data_blocks() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.data_blocks();
  }

  void add_data_block(const std::string &n) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.add_data_block(n);
  }

  void remove_data_block(std::size_t i) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.remove_data_block(i);
  }

  void remove_data_block(const std::string &n) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.remove_data_block(n);
  }

  void remove_all_data_blocks() {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.remove_all_data_blocks();
  }

  std::size_t file_size() const override {
    return std::atomic_load(&size_);
  }

  void grow(std::size_t bytes) {
    size_.fetch_add(bytes);
  }

  void shrink(std::size_t bytes) {
    size_.fetch_sub(bytes);
  }

 private:
  mutable std::shared_mutex mtx_;
  data_status dstatus_{};
  std::atomic<std::size_t> size_;
};

class directory_tree : public directory_service, public directory_management_service {
 public:
  directory_tree();

  void create_directory(const std::string &path) override;
  void create_directories(const std::string &path) override;

  void create_file(const std::string &path) override;

  bool exists(const std::string &path) const override;

  std::size_t file_size(const std::string &path) const override;

  std::time_t last_write_time(const std::string &path) const override;

  perms permissions(const std::string &path) override;
  void permissions(const std::string &path, const perms &permissions, perm_options opts) override;

  void remove(const std::string &path) override;
  void remove_all(const std::string &path) override;

  void rename(const std::string &old_path, const std::string &new_path) override;

  file_status status(const std::string &path) const override;

  std::vector<directory_entry> directory_entries(const std::string &path) override;

  std::vector<directory_entry> recursive_directory_entries(const std::string &path) override;

  data_status dstatus(const std::string &path) override;
  void dstatus(const std::string &path, const data_status &status) override;

  storage_mode mode(const std::string &path) override;

  std::string persistent_store_prefix(const std::string &path) override;

  std::vector<std::string> data_blocks(const std::string &path) override;

  bool is_regular_file(const std::string &path) override;
  bool is_directory(const std::string &path) override;

  void touch(const std::string &path) override;

  void grow(const std::string &path, std::size_t bytes) override;

  void shrink(const std::string &path, std::size_t bytes) override;

  void mode(const std::string &path, const storage_mode &mode) override;

  void persistent_store_prefix(const std::string &path, const std::string& prefix) override;

  void add_data_block(const std::string &path, const std::string &node) override;

  void remove_data_block(const std::string &path, std::size_t i) override;

  void remove_data_block(const std::string &path, const std::string &node) override;

  void remove_all_data_blocks(const std::string &path) override;

 private:
  std::shared_ptr<ds_node> get_node_unsafe(const std::string &path) const;

  std::shared_ptr<ds_node> get_node(const std::string &path) const;

  std::shared_ptr<ds_dir_node> get_node_as_dir(const std::string &path) const;

  std::shared_ptr<ds_file_node> get_node_as_file(const std::string &path) const;

  void touch(std::shared_ptr<ds_node> node, std::time_t time);

  std::shared_ptr<ds_dir_node> root_;
};

}
}

#endif //ELASTICMEM_DIRECTORY_SERVICE_SHARD_H
