#include "block.h"
#include "partition_manager.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

block::block(const std::string &id,
             const size_t capacity,
             const std::string &directory_host,
             const int directory_port,
             const std::string &auto_scaling_host,
             const int auto_scaling_port)
    : id_(id),
      manager_(capacity),
      impl_(partition_manager::build_partition(&manager_,
                                               "default",
                                               "default",
                                               "default",
                                               utils::property_map(),
                                               directory_host,
                                               directory_port,
                                               auto_scaling_host,
                                               auto_scaling_port)),
      directory_host_(directory_host),
      directory_port_(directory_port),
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
  return impl_;
}

void block::setup(const std::string &type,
                  const std::string &name,
                  const std::string &metadata,
                  const utils::property_map &conf) {
  impl_ = partition_manager::build_partition(&manager_,
                                             type,
                                             name,
                                             metadata,
                                             conf,
                                             directory_host_,
                                             directory_port_,
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
  std::string directory_host_ = "default";
  std::string auto_scaling_host_ = "default";
  int directory_port_ = 0;
  int auto_scaling_port_ = 0;
  utils::property_map conf;
  impl_.reset();
  impl_ = partition_manager::build_partition(&manager_,
                                             type,
                                             name,
                                             metadata,
                                             conf,
                                             directory_host_,
                                             directory_port_,
                                             auto_scaling_host_,
                                             auto_scaling_port_);
  if (impl_ == nullptr) {
    throw std::invalid_argument("No such type ");
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
