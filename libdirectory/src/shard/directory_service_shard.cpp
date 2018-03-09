#include "directory_service_shard.h"

#include <experimental/filesystem>
#include <iostream>
#include "../utils/directory_utils.h"

namespace elasticmem {
namespace directory {

directory_service_shard::directory_service_shard() : root_(std::make_shared<ds_dir_node>(std::string("/"))) {}

void directory_service_shard::create_directory(const std::string &path) {
  std::string ptemp = path;
  std::string directory_name = directory_utils::pop_path_element(ptemp);
  auto parent = get_node_as_dir(ptemp);
  if (parent->get_child(directory_name) == nullptr) {
    parent->add_child(std::make_shared<ds_dir_node>(directory_name));
  }
}

void directory_service_shard::create_directories(const std::string &path) {
  std::string p_so_far(root_->name());
  std::shared_ptr<ds_dir_node> dir_node = root_;
  for (auto &name: directory_utils::path_elements(path)) {
    directory_utils::push_path_element(p_so_far, name);
    std::shared_ptr<ds_node> child = dir_node->get_child(name);
    if (child == nullptr) {
      child = std::dynamic_pointer_cast<ds_node>(std::make_shared<ds_dir_node>(name));
      dir_node->add_child(child);
      dir_node = std::dynamic_pointer_cast<ds_dir_node>(child);
    } else {
      if (child->is_directory()) {
        dir_node = std::dynamic_pointer_cast<ds_dir_node>(child);
      } else {
        throw directory_service_exception("Cannot create directory: " + p_so_far + " is a file.");
      }
    }
  }
}

void directory_service_shard::create_file(const std::string &path) {
  namespace fs = std::experimental::filesystem;
  fs::path p(path);
  std::string filename = p.filename();
  if (filename == "." || filename == "/") {
    throw directory_service_exception("Path is a directory: " + path);
  }
  std::string parent_path = p.parent_path();
  auto node = get_node_unsafe(parent_path);
  if (node == nullptr) {
    create_directories(parent_path);
    node = get_node_unsafe(parent_path);
  } else if (node->is_regular_file()) {
    throw directory_service_exception(
        "Cannot create file in dir " + parent_path + ": " + node->name() + " is a file.");
  }

  auto parent = std::dynamic_pointer_cast<ds_dir_node>(node);
  auto child = std::dynamic_pointer_cast<ds_node>(std::make_shared<ds_file_node>(filename));
  parent->add_child(child);
}

bool directory_service_shard::exists(const std::string &path) const {
  return get_node_unsafe(path) != nullptr;
}

std::size_t directory_service_shard::file_size(const std::string &path) const {
  return get_node(path)->file_size();
}

std::time_t directory_service_shard::last_write_time(const std::string &path) const {
  return get_node(path)->last_write_time();
}

perms directory_service_shard::permissions(const std::string &path) {
  return get_node(path)->permissions();
}

void directory_service_shard::permissions(const std::string &path, const perms &prms, perm_options opts) {
  auto node = get_node(path);
  switch (opts) {
    case perm_options::replace: {
      node->permissions(prms & perms::mask);
      break;
    }
    case perm_options::add: {
      node->permissions(node->permissions() | (prms & perms::mask));
      break;
    }
    case perm_options::remove: {
      node->permissions(node->permissions() & ~(prms & perms::mask));
      break;
    }
  }
}

void directory_service_shard::remove(const std::string &path) {
  std::string ptemp = path;
  std::string child_name = directory_utils::pop_path_element(ptemp);
  auto parent = get_node_as_dir(ptemp);
  auto child = parent->get_child(child_name);
  if (child == nullptr) {
    throw directory_service_exception("Path does not exist: " + path);
  }
  if (child->is_directory() && !std::dynamic_pointer_cast<ds_dir_node>(child)->empty()) {
    throw directory_service_exception("Directory not empty: " + path);
  }
  parent->remove_child(child_name);
}

void directory_service_shard::remove_all(const std::string &path) {
  std::string ptemp = path;
  std::string child_name = directory_utils::pop_path_element(ptemp);
  auto parent = get_node_as_dir(ptemp);
  auto child = parent->get_child(child_name);
  if (child == nullptr) {
    throw directory_service_exception("Path does not exist: " + path);
  }
  parent->remove_child(child_name);
}

void directory_service_shard::rename(const std::string &old_path, const std::string &new_path) {
  if (old_path == new_path)
    return;
  std::string ptemp = old_path;
  std::string child_name = directory_utils::pop_path_element(ptemp);
  auto old_parent = get_node_as_dir(ptemp);
  auto old_child = old_parent->get_child(child_name);
  if (old_child == nullptr) {
    throw directory_service_exception("Path does not exist: " + old_path);
  }

  ptemp = new_path;
  child_name = directory_utils::pop_path_element(ptemp);
  auto new_parent = get_node_as_dir(ptemp);
  auto new_child = new_parent->get_child(child_name);
  if (new_child != nullptr) {
    if (new_child->is_directory())
      throw directory_service_exception("New path is a non-empty directory: " + new_path);
    new_parent->remove_child(child_name);
  }
  old_parent->remove_child(old_child->name());
  old_child->name(child_name);
  new_parent->add_child(old_child);
}

file_status directory_service_shard::status(const std::string &path) const {
  return get_node(path)->status();
}

std::vector<directory_entry> directory_service_shard::directory_entries(const std::string &path) {
  return get_node_as_dir(path)->entries();
}

std::vector<directory_entry> directory_service_shard::recursive_directory_entries(const std::string &path) {
  return get_node_as_dir(path)->recursive_entries();
}

data_status directory_service_shard::dstatus(const std::string &path) {
  return get_node_as_file(path)->dstatus();
}

storage_mode directory_service_shard::mode(const std::string &path) {
  return get_node_as_file(path)->mode();
}

std::vector<std::string> directory_service_shard::nodes(const std::string &path) {
  return get_node_as_file(path)->nodes();
}

bool directory_service_shard::is_regular_file(const std::string &path) {
  return get_node(path)->is_regular_file();
}

bool directory_service_shard::is_directory(const std::string &path) {
  return get_node(path)->is_directory();
}

void directory_service_shard::touch(const std::string &path) {
  touch(get_node(path), std::time(nullptr));
}

void directory_service_shard::grow(const std::string &path, std::size_t bytes) {
  get_node_as_file(path)->grow(bytes);
}

void directory_service_shard::shrink(const std::string &path, std::size_t bytes) {
  get_node_as_file(path)->shrink(bytes);
}

void directory_service_shard::dstatus(const std::string &path, const data_status &status) {
  get_node_as_file(path)->dstatus(status);
}

void directory_service_shard::mode(const std::string &path, const storage_mode &mode) {
  get_node_as_file(path)->mode(mode);
}

void directory_service_shard::add_node(const std::string &path, const std::string &node) {
  get_node_as_file(path)->add_node(node);
}

void directory_service_shard::remove_node(const std::string &path, std::size_t i) {
  get_node_as_file(path)->remove_node(i);
}

void directory_service_shard::remove_node(const std::string &path, const std::string &node) {
  get_node_as_file(path)->remove_node(node);
}

void directory_service_shard::clear_nodes(const std::string &path) {
  get_node_as_file(path)->clear_nodes();
}

std::shared_ptr<ds_node> directory_service_shard::get_node_unsafe(const std::string &path) const {
  std::shared_ptr<ds_node> node = root_;
  for (auto &name: directory_utils::path_elements(path)) {
    if (!node->is_directory()) {
      return nullptr;
    } else {
      node = std::dynamic_pointer_cast<ds_dir_node>(node)->get_child(name);
      if (node == nullptr) {
        return nullptr;
      }
    }
  }
  return node;
}

std::shared_ptr<ds_node> directory_service_shard::get_node(const std::string &path) const {
  auto node = get_node_unsafe(path);
  if (node == nullptr) {
    throw directory_service_exception("Path does not exist: " + path);
  }
  return node;
}

std::shared_ptr<ds_dir_node> directory_service_shard::get_node_as_dir(const std::string &path) const {
  auto node = get_node(path);
  if (node->is_regular_file()) {
    throw directory_service_exception("Path corresponds to a file: " + path);
  }
  return std::dynamic_pointer_cast<ds_dir_node>(node);
}

std::shared_ptr<ds_file_node> directory_service_shard::get_node_as_file(const std::string &path) const {
  auto node = get_node(path);
  if (!node->is_regular_file()) {
    throw directory_service_exception("Path corresponds to a directory: " + path);
  }
  return std::dynamic_pointer_cast<ds_file_node>(node);
}

void directory_service_shard::touch(std::shared_ptr<ds_node> node, std::time_t time) {
  node->last_write_time(time);
  if (node->is_regular_file()) {
    return;
  }
  auto dir = std::dynamic_pointer_cast<ds_dir_node>(node);
  for (const auto &child: *dir) {
    touch(child.second, time);
  }
}

}
}