#include "partition.h"

namespace jiffy {
namespace storage {

partition::partition(const std::vector<command> &supported_commands, std::string name)
    : supported_commands_(supported_commands),
      path_(""),
      name_(std::move(name)) {}

void partition::path(const std::string &path) {
  std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
  path_ = path;
}

const std::string &partition::path() const {
  std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
  return path_;
}

const std::string &partition::name() const {
  return name_; // Does not require locking since name does not change
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
