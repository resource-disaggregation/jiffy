#include "shared_log_block.h"
#include "jiffy/utils/logger.h"
#include <iostream>

namespace jiffy {
namespace storage {
using namespace utils;

shared_log_block::shared_log_block(std::size_t max_size, block_memory_allocator<char> alloc) : alloc_(alloc), max_(max_size) {
  data_ = alloc_.allocate(max_);
}

shared_log_block::~shared_log_block() {
  alloc_.deallocate(data_, max_);
}

shared_log_block::shared_log_block(const shared_log_block &other) {
  alloc_ = other.alloc_;
  max_ = other.max_;
  data_ = other.data_;
}

shared_log_block &shared_log_block::operator=(const shared_log_block &other) {
  alloc_ = other.alloc_;
  max_ = other.max_;
  data_ = other.data_;
  return *this;
}

bool shared_log_block::operator==(const shared_log_block &other) const {
  return data_ == other.data_ && alloc_ == other.alloc_ && max_ == other.max_;
}

std::pair<bool, std::string> shared_log_block::write(const std::string &data, std::size_t offset) {
  auto len = data.size();
  std::cout<<"offset="<<offset<<"data_="<<data_<<"\n";
  std::memcpy(data_ + offset, data.c_str(), len);
  return std::make_pair(true, std::string("!success"));
}

const std::pair<bool, std::string> shared_log_block::read(std::size_t offset, std::size_t size) const {
  if (offset >= max_) {
    throw std::invalid_argument("Read offset exceeds partition capacity");
  }
  return std::make_pair(true, std::string(data_ + offset, size));
}

std::size_t shared_log_block::size() const {
  return max_;
}

void shared_log_block::clear() {
  for (std::size_t i = 0; i < max_; i++) {
    data_[i] = 0;
  }
}

char *shared_log_block::data() const {
  return data_;
}

}
}

