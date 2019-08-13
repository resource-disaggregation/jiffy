#include "jiffy/directory/fs/ds_dir_node.h"
#include "jiffy/directory/fs/ds_file_node.h"
#include "jiffy/utils/time_utils.h"

namespace jiffy {
namespace directory {

ds_dir_node::ds_dir_node(const std::string &name)
    : ds_node(name, file_status(file_type::directory, perms(perms::all), utils::time_utils::now_ms())) {}

std::shared_ptr<ds_node> ds_dir_node::get_child(const std::string &name) const {
  std::unique_lock<std::mutex> lock(mtx_);
  auto ret = children_.find(name);
  if (ret != children_.end()) {
    return ret->second;
  } else {
    return nullptr;
  }
}

void ds_dir_node::add_child(std::shared_ptr<ds_node> node) {
  std::unique_lock<std::mutex> lock(mtx_);
  if (children_.find(node->name()) == children_.end()) {
    children_.insert(std::make_pair(node->name(), node));
  } else {
    throw directory_ops_exception("Child node already exists: " + node->name());
  }
}

void ds_dir_node::remove_child(const std::string &name) {
  std::unique_lock<std::mutex> lock(mtx_);
  auto ret = children_.find(name);
  if (ret != children_.end()) {
    children_.erase(ret);
  } else {
    throw directory_ops_exception("Child node not found: " + name);
  }
}

bool ds_dir_node::handle_lease_expiry(std::vector<std::string> &cleared_blocks,
                                      const std::string &child_name,
                                      std::shared_ptr<storage::storage_management_ops> storage) {
  std::unique_lock<std::mutex> lock(mtx_);
  auto ret = children_.find(child_name);
  if (ret != children_.end()) {
    if (ret->second->is_regular_file()) {
      auto file = std::dynamic_pointer_cast<ds_file_node>(ret->second);
      if (file->handle_lease_expiry(cleared_blocks, storage)) {
        children_.erase(ret);
        return true;
      }
      return false;
    } else if (ret->second->is_directory()) {
      auto dir = std::dynamic_pointer_cast<ds_dir_node>(ret->second);
      bool cleared = true;
      auto entries = dir->children();
      for (auto &entry: entries) {
        if (!dir->handle_lease_expiry(cleared_blocks, entry.first, storage)) {
          cleared = false;
        }
      }
      if (cleared)
        children_.erase(ret);
      return cleared;
    }
  } else {
    throw directory_ops_exception("Child node not found: " + child_name);
  }
  return false;
}

void ds_dir_node::sync(const std::string &backing_path, const std::shared_ptr<storage::storage_management_ops> &storage) {
  std::unique_lock<std::mutex> lock(mtx_);
  for (const auto &entry: children_) {
    entry.second->sync(backing_path, storage);
  }
}

void ds_dir_node::dump(std::vector<std::string> &cleared_blocks,
                       const std::string &backing_path,
                       const std::shared_ptr<storage::storage_management_ops> &storage) {
  std::unique_lock<std::mutex> lock(mtx_);
  for (const auto &entry: children_) {
    entry.second->dump(cleared_blocks, backing_path, storage);
  }
}

void ds_dir_node::load(const std::string &path,
                       const std::string &backing_path,
                       const std::shared_ptr<storage::storage_management_ops> &storage,
                       const std::shared_ptr<block_allocator> &allocator) {
  std::unique_lock<std::mutex> lock(mtx_);
  for (const auto &entry: children_) {
    entry.second->load(path, backing_path, storage, allocator);
  }
}

std::vector<directory_entry> ds_dir_node::entries() const {
  std::unique_lock<std::mutex> lock(mtx_);
  std::vector<directory_entry> ret;
  ret.reserve(children_.size());
  populate_entries(ret);
  return ret;
}

std::vector<directory_entry> ds_dir_node::recursive_entries() const {
  std::unique_lock<std::mutex> lock(mtx_);
  std::vector<directory_entry> ret;
  populate_recursive_entries(ret);
  return ret;
}

std::vector<std::string> ds_dir_node::child_names() const {
  std::vector<std::string> ret;
  for (const auto &entry: children_) {
    ret.push_back(entry.first);
  }
  return ret;
}

ds_dir_node::child_map ds_dir_node::children() const {
  return children_;
}

void ds_dir_node::populate_entries(std::vector<directory_entry> &entries) const {
  for (auto &entry: children_) {
    entries.emplace_back(entry.second->entry());
  }
}

void ds_dir_node::populate_recursive_entries(std::vector<directory_entry> &entries) const {
  for (auto &entry: children_) {
    entries.emplace_back(entry.second->entry());
    if (entry.second->is_directory()) {
      std::dynamic_pointer_cast<ds_dir_node>(entry.second)->populate_recursive_entries(entries);
    }
  }
}

}
}
