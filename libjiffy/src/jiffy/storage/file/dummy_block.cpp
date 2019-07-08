#include "dummy_block.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace storage {
using namespace utils;

dummy_block::dummy_block(std::size_t max_size, block_memory_allocator<char> alloc) : alloc_(alloc), max_(max_size) {
  extend_ = false;
  data_ = alloc_.allocate(max_);
  tail_ = 0;
}

dummy_block::~dummy_block() {
  alloc_.deallocate(data_, max_);
}

dummy_block::dummy_block(const dummy_block &other) {
  alloc_ = other.alloc_;
  max_ = other.max_;
  data_ = other.data_;
  tail_ = other.tail_;
  extend_ = other.extend_;
}

dummy_block &dummy_block::operator=(const dummy_block &other) {
  alloc_ = other.alloc_;
  max_ = other.max_;
  data_ = other.data_;
  tail_ = other.tail_;
  extend_ = other.extend_;
  return *this;
}

bool dummy_block::operator==(const dummy_block &other) const {
  return data_ == other.data_ && tail_ == other.tail_ && alloc_ == other.alloc_ && max_ == other.max_
      && extend_ == other.extend_;
}

std::pair<bool, std::string> dummy_block::write(const std::string &msg, std::size_t offset) {
  std::size_t len = msg.size();
  if (len == 0) {
    return std::make_pair(true, std::string("!success"));
  }
  if (len + offset <= max_) {
    std::memcpy(data_ + offset, msg.c_str(), len);
    if(len + offset > tail_)
      tail_ = len + offset;
    return std::make_pair(true, std::string("!success"));
  } else {
    std::size_t remain_len = max_ - offset;
    std::memcpy(data_ + offset, msg.c_str(), remain_len);
    tail_ = max_;
    extend_ = true;
    return std::make_pair(false, msg.substr(remain_len, msg.size() - remain_len));
  }
}

const std::pair<bool, std::string> dummy_block::read(std::size_t offset, std::size_t size) const {
  if (offset >= tail_) {
    return std::make_pair(false, std::string("!not_available"));
  }
  if (offset + size <= tail_) {
    return std::make_pair(true, std::string((const char *) (data_ + offset), size));
  } else if (tail_ == max_) {
    return std::make_pair(false,
                          std::string((const char *) (data_ + offset),
                                      max_ - offset));
  } else {
    return std::make_pair(true, std::string((const char *) (data_ + offset), tail_ - offset));
  }
}

std::size_t dummy_block::size() const {
  return tail_;
}

std::size_t dummy_block::capacity() {
  return max_;
}

void dummy_block::clear() {
  tail_ = 0;
  extend_ = false;
}

bool dummy_block::empty() const {
  return tail_ == 0;
}

std::size_t dummy_block::max_offset() const {
  return max_;
}

char *dummy_block::data() const {
  return data_;
}

}
}

