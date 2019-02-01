#include "block.h"
#include "partition_manager.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

block::block(const std::string &id, const size_t capacity, const std::string &directory_host, int directory_port)
    : id_(id), manager_(capacity), impl_(nullptr), directory_host_(directory_host), directory_port_(directory_port) {
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
  impl_ = partition_manager::build_partition(&manager_, type, name, metadata, conf);
  if (impl_ == nullptr) {
    throw std::invalid_argument("No such type " + type);
  }
}

void block::destroy() {
  impl_.reset();
  impl_ = nullptr;
}

size_t block::capacity() const {
  return manager_.mb_capacity();
}

size_t block::used() const {
  return manager_.mb_used();
}

}
}
