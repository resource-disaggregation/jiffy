#include "partition.h"

namespace jiffy {
namespace storage {

partition::partition(block_memory_manager *manager,
                     const std::string &name,
                     const std::string &metadata,
                     const command_map &supported_commands)
    : name_(name),
      metadata_(metadata),
      supported_commands_(supported_commands),
      manager_(manager),
      binary_allocator_(build_allocator<uint8_t>()) {
  default_ = supported_commands_.empty();
}

void partition::path(const std::string &path) {
  path_ = path;
}

const std::string &partition::path() const {
  return path_;
}

void partition::name(const std::string &name) {
  name_ = name;
}

const std::string &partition::name() const {
  return name_;
}

void partition::metadata(const std::string &metadata) {
  metadata_ = metadata;
}

const std::string &partition::metadata() const {
  return metadata_;
}

bool partition::is_accessor(const std::string &cmd) const {
  // Does not require lock since block_ops don't change
  return supported_commands_.at(cmd).is_accessor();
}

bool partition::is_mutator(const std::string &cmd) const {
  // Does not require lock since block_ops don't change
  return supported_commands_.at(cmd).is_mutator();
}

uint32_t partition::command_id(const std::string &cmd_name) {
  auto it = supported_commands_.find(cmd_name);
  if (it == supported_commands_.end())
    return UINT32_MAX;
  return it->second.id;
}

std::size_t partition::storage_capacity() {
  return manager_->mb_capacity();
}

std::size_t partition::storage_size() {
  return manager_->mb_used();
}

subscription_map &partition::subscriptions() {
  return sub_map_;
}

block_response_client_map &partition::clients() {
  return client_map_;
}

void partition::set_name_and_metadata(const std::string &name, const std::string &metadata) {
  name_ = name;
  metadata_ = metadata;
}

binary partition::make_binary(const std::string &str) {
  return binary(str, binary_allocator_);
}

}
}
