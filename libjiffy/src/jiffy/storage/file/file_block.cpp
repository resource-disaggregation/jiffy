#include "file_block.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace storage {
using namespace utils;

file_block::file_block(std::size_t max_size, block_memory_allocator<char> alloc) : alloc_(alloc), max_(max_size) {
  data_ = alloc_.allocate(max_);
}

file_block::~file_block() {
  alloc_.deallocate(data_, max_);
}

file_block::file_block(const file_block &other) {
  alloc_ = other.alloc_;
  max_ = other.max_;
  data_ = other.data_;
}

file_block &file_block::operator=(const file_block &other) {
  alloc_ = other.alloc_;
  max_ = other.max_;
  data_ = other.data_;
  return *this;
}

bool file_block::operator==(const file_block &other) const {
  return data_ == other.data_ && alloc_ == other.alloc_ && max_ == other.max_;
}

std::pair<bool, std::string> file_block::write(const std::string &data, std::size_t offset) {
  auto len = data.size();
  std::memcpy(data_ + offset, data.c_str(), len);
  return std::make_pair(true, std::string("!success"));
}

const std::pair<bool, std::string> file_block::read(std::size_t offset, std::size_t size) const {
  if (offset >= max_) {
    throw std::invalid_argument("Read offset exceeds partition capacity");
  }
  return std::make_pair(true, std::string(data_ + offset, size));
}

std::size_t file_block::size() const {
  return max_;
}

void file_block::clear() {
  for (std::size_t i = 0; i < max_; i++) {
    data_[i] = 0;
  }
}

char *file_block::data() const {
  return data_;
}

}
}

