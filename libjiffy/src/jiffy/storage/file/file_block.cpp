#include "file_block.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace storage {
using namespace utils;

file_block::file_block(std::size_t max_size, block_memory_allocator<char> alloc) : alloc_(alloc), max_(max_size) {
  extend_ = false;
  data_ = alloc_.allocate(max_);
  tail_ = 0;
}

file_block::~file_block() {
  alloc_.deallocate(data_, max_);
}

file_block::file_block(const file_block &other) {
  alloc_ = other.alloc_;
  max_ = other.max_;
  data_ = other.data_;
  tail_ = other.tail_;
  extend_ = other.extend_;
}

file_block &file_block::operator=(const file_block &other) {
  alloc_ = other.alloc_;
  max_ = other.max_;
  data_ = other.data_;
  tail_ = other.tail_;
  extend_ = other.extend_;
  return *this;
}

bool file_block::operator==(const file_block &other) const {
  return data_ == other.data_ && tail_ == other.tail_ && alloc_ == other.alloc_ && max_ == other.max_
      && extend_ == other.extend_;
}

std::pair<bool, std::string> file_block::write(const std::string &data, std::size_t offset) {
  auto len = data.size();
  if (len == 0) return std::make_pair(true, std::string("!success"));
  else if (len + offset <= max_) {
    std::memcpy(data_ + offset, data.c_str(), len);
    tail_ = std::max(tail_, len + offset);
    return std::make_pair(true, std::string("!success"));
  } else {
    auto remain_len = max_ - offset;
    std::memcpy(data_ + offset, data.c_str(), remain_len);
    tail_ = max_;
    extend_ = true;
    return std::make_pair(false, data.substr(remain_len, len - remain_len));
  }
}

const std::pair<bool, std::string> file_block::read(std::size_t offset, std::size_t size) const {
  if (offset >= tail_) return std::make_pair(false, std::string("!not_available"));
  else if (offset + size <= tail_) return std::make_pair(true, std::string(data_ + offset, size));
  else return std::make_pair(tail_ != max_, std::string(data_ + offset, tail_ - offset));
}

std::size_t file_block::size() const {
  return tail_;
}

std::size_t file_block::capacity() {
  return max_;
}

void file_block::clear() {
  tail_ = 0;
  extend_ = false;
}

bool file_block::empty() const {
  return tail_ == 0;
}

char *file_block::data() const {
  return data_;
}

}
}

