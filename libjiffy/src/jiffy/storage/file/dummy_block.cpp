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
std::pair<bool, std::string> dummy_block::push_back(const std::string &msg) {
  std::size_t len = msg.size();
  if (len == 0) {
    return std::make_pair(true, std::string("!success"));
  }
  if (len + tail_ <= max_) {
    std::memcpy(data_ + tail_, msg.c_str(), len);
    tail_ += len;
    return std::make_pair(true, std::string("!success"));
  } else if (max_ >= tail_) {
    std::size_t remain_len = max_ - tail_;
    std::memcpy(data_ + tail_, msg.c_str(), remain_len);
    tail_ += remain_len;
    extend_ = true;
    return std::make_pair(false, msg.substr(remain_len, msg.size() - remain_len));
  } else {
    //throw std::logic_error("This partition is full, should be detected by partition");
    return std::make_pair(false, msg);
  }
}

const std::pair<bool, std::string> dummy_block::read(std::size_t offset, std::size_t size) const {
  if (offset >= max_) {
    return std::make_pair(false, std::string("!reach_end"));
  }
  if (offset >= tail_ || empty()) {
    return std::make_pair(false, std::string("!not_available"));
  }
  if (offset + size <= max_) {
    return std::make_pair(true, std::string((const char *) (data_ + offset), size));
  } else if (extend_) {
    return std::make_pair(false,
                          std::string((const char *) (data_ + offset),
                                      max_ - offset));
  } else {
    throw std::logic_error("Invalid offset");
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

