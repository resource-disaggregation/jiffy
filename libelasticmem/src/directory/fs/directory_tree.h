#ifndef ELASTICMEM_DIRECTORY_SERVICE_SHARD_H
#define ELASTICMEM_DIRECTORY_SERVICE_SHARD_H

#include <utility>
#include <memory>
#include <atomic>
#include <map>
#include <shared_mutex>

#include "../directory_ops.h"
#include "../block/block_allocator.h"
#include "../../utils/time_utils.h"
#include "../../storage/storage_management_ops.h"
#include "../../utils/directory_utils.h"

namespace elasticmem {
namespace directory {

class lease_expiry_worker;

class ds_node {
 public:
  explicit ds_node(std::string name, file_status status)
      : name_(std::move(name)), status_(status) {}

  virtual ~ds_node() = default;

  const std::string &name() const { return name_; }

  void name(const std::string &name) { name_ = name; }

  bool is_directory() const { return status_.type() == file_type::directory; }

  bool is_regular_file() const { return status_.type() == file_type::regular; }

  file_status status() const { return status_; }

  directory_entry entry() const { return directory_entry(name_, status_); }

  std::uint64_t last_write_time() const { return status_.last_write_time(); }

  void permissions(const perms &prms) { status_.permissions(prms); }

  perms permissions() const { return status_.permissions(); }

  void last_write_time(std::uint64_t time) { status_.last_write_time(time); }

  virtual void flush(const std::string &path,
                     const std::shared_ptr<storage::storage_management_ops> &storage,
                     std::shared_ptr<block_allocator> alloc) = 0;

 private:
  std::string name_{};
  file_status status_{};
};

class ds_file_node : public ds_node {
 public:
  explicit ds_file_node(const std::string &name)
      : ds_node(name, file_status(file_type::regular, perms(perms::all), utils::time_utils::now_ms())),
        dstatus_{} {}

  ds_file_node(const std::string &name,
               storage_mode mode,
               const std::string &persistent_store_prefix,
               std::size_t chain_length,
               std::vector<block_chain> blocks) :
      ds_node(name, file_status(file_type::regular, perms(perms::all), utils::time_utils::now_ms())),
      dstatus_(mode, persistent_store_prefix, chain_length, std::move(blocks)) {}

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

  void persistent_store_prefix(const std::string &prefix) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.persistent_store_prefix(prefix);
  }

  std::size_t chain_length() const {
    return dstatus_.chain_length();
  }

  void chain_length(std::size_t chain_length) {
    dstatus_.chain_length(chain_length);
  }

  const std::vector<block_chain> &data_blocks() const {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return dstatus_.data_blocks();
  }

  void flush(const std::string &path,
             const std::shared_ptr<storage::storage_management_ops> &storage,
             std::shared_ptr<block_allocator> alloc) override {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    dstatus_.mode(storage_mode::flushing);
    for (const auto &block: dstatus_.data_blocks()) {
      storage->flush(block.tail(), dstatus_.persistent_store_prefix(), path);
      alloc->free(block.block_names);
    }
    dstatus_.remove_all_data_blocks();
    dstatus_.mode(storage_mode::on_disk);
  }

 private:
  mutable std::shared_mutex mtx_;
  data_status dstatus_{};
};

class ds_dir_node : public ds_node {
 public:
  typedef std::map<std::string, std::shared_ptr<ds_node>> child_map;
  explicit ds_dir_node(const std::string &name)
      : ds_node(name, file_status(file_type::directory, perms(perms::all), utils::time_utils::now_ms())) {}

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
      throw directory_ops_exception("Child node already exists: " + node->name());
    }
  }

  void remove_child(const std::string &name) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    auto ret = children_.find(name);
    if (ret != children_.end()) {
      children_.erase(ret);
    } else {
      throw directory_ops_exception("Child node not found: " + name);
    }
  }

  void flush(const std::string &path,
             const std::shared_ptr<storage::storage_management_ops> &storage,
             std::shared_ptr<block_allocator> alloc) override {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    for (const auto &entry: children_) {
      std::string child_path = path;
      utils::directory_utils::push_path_element(child_path, entry.first);
      entry.second->flush(child_path, storage, alloc);
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

  std::vector<std::string> children() const {
    std::vector<std::string> ret;
    for (const auto &entry: children_) {
      ret.push_back(entry.first);
    }
    return ret;
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

class directory_tree : public directory_ops, public directory_management_ops {
 public:
  explicit directory_tree(std::shared_ptr<block_allocator> allocator,
                          std::shared_ptr<storage::storage_management_ops> storage);

  void create_directory(const std::string &path) override;
  void create_directories(const std::string &path) override;

  data_status open(const std::string &path) override;
  data_status create(const std::string &path,
                     const std::string &persistent_store_prefix,
                     std::size_t num_blocks,
                     std::size_t chain_length) override;

  bool exists(const std::string &path) const override;

  std::uint64_t last_write_time(const std::string &path) const override;

  perms permissions(const std::string &path) override;
  void permissions(const std::string &path, const perms &permissions, perm_options opts) override;

  void remove(const std::string &path) override;
  void remove_all(const std::string &path) override;

  void flush(const std::string &path) override;

  void rename(const std::string &old_path, const std::string &new_path) override;

  file_status status(const std::string &path) const override;

  std::vector<directory_entry> directory_entries(const std::string &path) override;

  std::vector<directory_entry> recursive_directory_entries(const std::string &path) override;

  data_status dstatus(const std::string &path) override;

  bool is_regular_file(const std::string &path) override;
  bool is_directory(const std::string &path) override;

  void touch(const std::string &path) override;
  block_chain resolve_failures(const std::string &path, const block_chain &chain) override;
  block_chain add_blocks_to_chain(const std::string &path, const block_chain &chain, std::size_t count) override;

 private:
  std::shared_ptr<ds_node> get_node_unsafe(const std::string &path) const;

  std::shared_ptr<ds_node> get_node(const std::string &path) const;

  std::shared_ptr<ds_dir_node> get_node_as_dir(const std::string &path) const;

  std::shared_ptr<ds_file_node> get_node_as_file(const std::string &path) const;

  std::shared_ptr<ds_node> touch_node_path(const std::string &path, std::uint64_t time) const;

  void clear_storage(std::shared_ptr<ds_node> node);

  void touch(std::shared_ptr<ds_node> node, std::uint64_t time);

  std::shared_ptr<ds_dir_node> root_;
  std::shared_ptr<block_allocator> allocator_;
  std::shared_ptr<storage::storage_management_ops> storage_;

  friend class lease_expiry_worker;
};

}
}

#endif //ELASTICMEM_DIRECTORY_SERVICE_SHARD_H
