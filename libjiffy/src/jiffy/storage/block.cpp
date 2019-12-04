#include "block.h"
#include "partition_manager.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

block::block(const std::string &id,
             const size_t capacity,
             const std::string &auto_scaling_host,
             const int auto_scaling_port)
    : id_(id),
      manager_(capacity),
      impl_(partition_manager::build_partition(&manager_,
                                               client_map_,
                                               "default",
                                               "default",
                                               "default",
                                               utils::property_map(),
                                               auto_scaling_host,
                                               auto_scaling_port)),
      auto_scaling_host_(auto_scaling_host),
      auto_scaling_port_(auto_scaling_port) {
  if (impl_ == nullptr) {
    throw std::invalid_argument("No such type ");
  }
}

const std::string &block::id() const {
  return id_;
}

std::shared_ptr<chain_module> block::impl() {
  if (impl_ == nullptr) {
    throw std::logic_error("De-referenced uninitialized partition implementation");
  }
  return std::atomic_load(&impl_);
}

void block::setup(const std::string &type,
                  const std::string &name,
                  const std::string &metadata,
                  const utils::property_map &conf) {
  impl_ = partition_manager::build_partition(&manager_,
                                             client_map_,
                                             type,
                                             name,
                                             metadata,
                                             conf,
                                             auto_scaling_host_,
                                             auto_scaling_port_);
  if (impl_ == nullptr) {
    throw std::invalid_argument("No such type " + type);
  }
}

void block::destroy() {
  LOG(log_level::info) << "Destroying partition " << impl_->name() << " on block " << id_;
  std::string type = "default";
  std::string name = "default";
  std::string metadata = "default";
  std::string auto_scaling_host_ = "default";
  int auto_scaling_port_ = 0;
  utils::property_map conf;
  auto new_partition = partition_manager::build_partition(&manager_,
                                             client_map_,
                                             type,
                                             name,
                                             metadata,
                                             conf,
                                             auto_scaling_host_,
                                             auto_scaling_port_);
  std::atomic_store(&impl_, new_partition);
  if (impl_ == nullptr) {
    throw std::invalid_argument("Fail to set default partition");
  }
}

size_t block::capacity() const {
  return manager_.mb_capacity();
}

size_t block::used() const {
  return manager_.mb_used();
}

block::operator bool() const noexcept {
  return impl_ != nullptr;
}

bool block::valid() const {
  return impl_ != nullptr;
}

}
}
