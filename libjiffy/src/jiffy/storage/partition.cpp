#include "partition.h"

namespace jiffy {
namespace storage {

partition::partition(block_memory_manager *manager,
                     const std::string &name,
                     const std::string &metadata,
                     const std::vector<command> &supported_commands)
    : name_(name),
      metadata_(metadata),
      supported_commands_(supported_commands),
      manager_(manager),
      binary_allocator_(build_allocator<uint8_t>()) {
  default_ = supported_commands_.empty();
}

void partition::path(const std::string &path) {
  std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
  path_ = path;
}

const std::string &partition::path() const {
  std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
  return path_;
}

void partition::name(const std::string &name) {
  std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
  name_ = name;
}

const std::string &partition::name() const {
  return name_;
}

void partition::metadata(const std::string &metadata) {
  std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
  metadata_ = metadata;
}

const std::string &partition::metadata() const {
  return metadata_;
}

bool partition::is_accessor(int i) const {
  // Does not require lock since block_ops don't change
  return supported_commands_.at(static_cast<size_t>(i)).type == accessor;
}

bool partition::is_mutator(int i) const {
  // Does not require lock since block_ops don't change
  return supported_commands_.at(static_cast<size_t>(i)).type == mutator;
}

std::string partition::command_name(int cmd_id) {
  if (!default_.load())
    return supported_commands_[cmd_id].name;
  else return "default_partition";
}

std::size_t partition::storage_capacity() {
  return manager_->mb_capacity();
}

std::size_t partition::storage_size() {
  return manager_->mb_used();
}

subscription_map &partition::subscriptions() {
  std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
  return sub_map_;
}

block_response_client_map &partition::clients() {
  std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
  return client_map_;
}

void partition::set_name_and_metadata(const std::string &name, const std::string &metadata) {
  std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
  name_ = name;
  metadata_ = metadata;
}

binary partition::make_binary(const std::string &str) {
  return binary(str, binary_allocator_);
}

}
}
