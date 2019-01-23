#include <ldap.h>
#include "partition.h"

namespace jiffy {
namespace storage {

partition::partition(block_memory_manager *manager,
                     const std::string &name,
                     const std::string &metadata,
                     const std::vector<command> &supported_commands)
    : name_(name), metadata_(metadata), supported_commands_(supported_commands), manager_(manager) {
  bytes_ = 0;
}

void partition::path(const std::string &path) {
  std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
  path_ = path;
}

const std::string &partition::path() const {
  std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
  return path_;
}

const std::string &partition::name() const {
  return name_;
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
  // Does not require lock since block_ops don't change
  return supported_commands_[cmd_id].name;
}

std::size_t partition::storage_capacity() {
  return manager_->mb_capacity();
}

std::size_t partition::storage_size() {
  return bytes_.load();
}

subscription_map &partition::subscriptions() {
  std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
  return sub_map_;
}

block_response_client_map &partition::clients() {
  std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
  return client_map_;
}

}
}
