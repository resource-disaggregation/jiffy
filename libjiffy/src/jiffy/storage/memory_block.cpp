#include "memory_block.h"
#include "partition_manager.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

memory_block::memory_block(const std::string &id, const size_t capacity)
    : id_(id), capacity_(capacity), impl_(nullptr) {
}

const std::string &memory_block::id() const {
  return id_;
}

std::shared_ptr<chain_module> memory_block::impl() {
  if (impl_ == nullptr) {
    throw std::logic_error("De-referenced uninitialized partition implementation");
  }
  return impl_;
}

void memory_block::setup(const std::string &type,
                         const std::string &name,
                         const std::string &metadata,
                         const utils::property_map &conf) {
  impl_ = partition_manager::build_partition(type, name, metadata, conf);
  if (impl_ == nullptr) {
    throw std::invalid_argument("No such type " + type);
  }
  impl_->set_capacity(capacity_);
}

void memory_block::destroy() {
  impl_.reset();
  impl_ = nullptr;
}

size_t memory_block::capacity() const {
  return capacity_;
}

}
}
