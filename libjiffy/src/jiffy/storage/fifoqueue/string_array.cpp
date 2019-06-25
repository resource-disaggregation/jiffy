#include "string_array.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace storage {
using namespace utils;

string_array::string_array(std::size_t max_size, block_memory_allocator<char> alloc) : alloc_(alloc), max_(max_size) {
  split_string_ = false;
  data_ = alloc_.allocate(max_);
  tail_ = 0;
  last_element_offset_ = 0;
}

string_array::~string_array() {
  alloc_.deallocate(data_, max_);
}

string_array::string_array(const string_array &other) {
  split_string_ = other.split_string_.load();
  alloc_ = other.alloc_;
  max_ = other.max_;
  data_ = other.data_;
  tail_ = other.tail_;
  last_element_offset_ = other.last_element_offset_;
}

string_array &string_array::operator=(const string_array &other) {
  split_string_ = other.split_string_.load();
  alloc_ = other.alloc_;
  max_ = other.max_;
  data_ = other.data_;
  tail_ = other.tail_;
  last_element_offset_ = other.last_element_offset_;
  return *this;
}

bool string_array::operator==(const string_array &other) const {
  return data_ == other.data_ && tail_ == other.tail_ && alloc_ == other.alloc_ && max_ == other.max_
      && last_element_offset_ == other.last_element_offset_;
}

std::pair<bool, std::string> string_array::push_back(const std::string &msg) {
  std::size_t len = msg.size();
  if (len + tail_ + metadata_length <= max_) {
    char metadata[metadata_length];
    std::memset(metadata, 0, metadata_length);
    std::memcpy(data_ + tail_, (char *) &len, metadata_length);
    last_element_offset_ = tail_;
    tail_ += metadata_length;
    std::memcpy(data_ + tail_, msg.c_str(), len);
    tail_ += len;
    return std::make_pair(true, std::string("!success"));
  } else if (max_ - tail_ >= metadata_length) {
    split_string_ = true;
    std::size_t remain_len = max_ - tail_ - metadata_length;
    std::memcpy(data_ + tail_, (char *) &len, metadata_length);
    last_element_offset_ = tail_;
    tail_ += metadata_length;
    std::memcpy(data_ + tail_, msg.c_str(), remain_len);
    tail_ += remain_len;
    return std::make_pair(false, msg.substr(remain_len, msg.size() - remain_len));
  } else {
    return std::make_pair(false, msg);
  }
}

const std::pair<bool, std::string> string_array::at(std::size_t offset) const {
  if (offset > max_ - metadata_length) {
    return std::make_pair(false, std::string("!reach_end"));
  }
  if (offset > last_element_offset_ || empty()) {
    return std::make_pair(false, std::string("!not_available"));
  }
  std::size_t len = *((std::size_t *) (data_ + offset));
  if (offset + metadata_length + len <= max_) {
    return std::make_pair(true, std::string((const char *) (data_ + offset + metadata_length), len));
  } else if (split_string_.load()) {
    return std::make_pair(false,
                          std::string((const char *) (data_ + offset + metadata_length),
                                      max_ - offset - metadata_length));
  } else {
    throw std::logic_error("Invalid offset");
  }
}

std::size_t string_array::find_next(std::size_t offset) const {
  if (offset >= last_element_offset_) {
    return 0;
  }
  std::size_t len = *((std::size_t *) (data_ + offset));
  return offset + len + metadata_length;
}

std::size_t string_array::size() const {
  return tail_;
}

std::size_t string_array::capacity() {
  return max_ - metadata_length;
}

bool string_array::split_last_string() {
  return split_string_.load();
}

void string_array::clear() {
  tail_ = 0;
  last_element_offset_ = 0;
  split_string_ = false;
}

bool string_array::empty() const {
  return tail_ == 0;
}

string_array::iterator string_array::begin() {
  return string_array::iterator(*this, 0);
}

string_array::iterator string_array::end() {
  return string_array::iterator(*this, max_);
}

std::size_t string_array::max_offset() const {
  return max_;
}

string_array::const_iterator string_array::begin() const {
  return string_array::const_iterator(*this, 0);
}

string_array::const_iterator string_array::end() const {
  return string_array::const_iterator(*this, max_);
}

string_array_iterator::string_array_iterator(string_array &impl, std::size_t pos)
    : impl_(impl),
      pos_(pos) {}

string_array_iterator::value_type string_array_iterator::operator*() const {
  auto ret = impl_.at(pos_);
  return ret.second;
}

const string_array_iterator string_array_iterator::operator++(int) {
  pos_ = impl_.find_next(pos_);
  if (pos_) {
    return *this;
  } else {
    pos_ = impl_.max_offset();
    return *this;
  }
}

bool string_array_iterator::operator==(string_array_iterator other) const {
  return (impl_ == other.impl_) && (pos_ == other.pos_);
}

bool string_array_iterator::operator!=(string_array_iterator other) const {
  return !(*this == other);
}

string_array_iterator &string_array_iterator::operator=(const string_array_iterator &other) {
  impl_ = other.impl_;
  pos_ = other.pos_;
  return *this;
}

const_string_array_iterator::const_string_array_iterator(const string_array &impl, std::size_t pos)
    : impl_(impl), pos_(pos) {}

const_string_array_iterator::value_type const_string_array_iterator::operator*() const {
  auto ret = impl_.at(pos_);
  return ret.second;
}

const const_string_array_iterator const_string_array_iterator::operator++(int) {
  pos_ = impl_.find_next(pos_);
  if (pos_) {
    return *this;
  } else {
    pos_ = impl_.max_offset();
    return *this;
  }
}

bool const_string_array_iterator::operator==(const_string_array_iterator other) const {
  return (impl_ == other.impl_) && (pos_ == other.pos_);
}

bool const_string_array_iterator::operator!=(const_string_array_iterator other) const {
  return !(*this == other);
}

}
}

