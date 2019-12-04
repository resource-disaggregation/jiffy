#include "string_array.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace storage {
using namespace utils;

string_array::string_array(std::size_t max_size, block_memory_allocator<char> alloc) : alloc_(alloc), max_(max_size) {
  data_ = alloc_.allocate(max_);
  tail_ = 0;
  last_element_offset_ = 0;
  split_string_ = false;
}

string_array::~string_array() {
  alloc_.deallocate(data_, max_);
}

string_array::string_array(const string_array &other) {
  alloc_ = other.alloc_;
  max_ = other.max_;
  data_ = other.data_;
  tail_ = other.tail_;
  split_string_ = other.split_string_;
  last_element_offset_ = other.last_element_offset_;
}

string_array &string_array::operator=(const string_array &other) {
  alloc_ = other.alloc_;
  max_ = other.max_;
  data_ = other.data_;
  tail_ = other.tail_;
  last_element_offset_ = other.last_element_offset_;
  split_string_ = other.split_string_;
  return *this;
}

bool string_array::operator==(const string_array &other) const {
  return data_ == other.data_ && tail_ == other.tail_ && alloc_ == other.alloc_ && max_ == other.max_
      && last_element_offset_ == other.last_element_offset_
      && split_string_ == other.split_string_;
}

std::pair<bool, std::string> string_array::push_back(const std::string &item) {
  auto len = item.size();
  if (len + tail_ + METADATA_LEN <= max_ && !split_string_) { // Complete item will be written
    // Write length
    std::memcpy(data_ + tail_, (char *) &len, METADATA_LEN);
    last_element_offset_ = tail_;
    tail_ += METADATA_LEN;

    // Write data
    std::memcpy(data_ + tail_, item.c_str(), len);
    tail_ += len;
    return std::make_pair(true, std::string("!success"));
  } else { // Item will not be written, full item will be returned
    split_string_ = true;
    return std::make_pair(false, item);
  }
}

const std::pair<bool, std::string> string_array::at(std::size_t offset) const {
  if (offset > last_element_offset_ || empty()) {
    if (split_string_)
      return std::make_pair(false, "");
    return std::make_pair(false, std::string("!not_available"));
  }
  auto len = *((std::size_t *) (data_ + offset));
  return std::make_pair(true, std::string(data_ + offset + METADATA_LEN, len));
}

std::size_t string_array::find_next(std::size_t offset) const {
  if (offset >= last_element_offset_ || offset >= tail_) return 0;
  return offset + *reinterpret_cast<size_t*>(data_ + offset) + METADATA_LEN;
}

std::size_t string_array::size() const {
  return tail_;
}

std::size_t string_array::last_element_offset() const {
  return last_element_offset_;
}

std::size_t string_array::capacity() {
  return max_ - METADATA_LEN;
}

void string_array::clear() {
  tail_ = 0;
  last_element_offset_ = 0;
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
bool string_array::full() const {
  return split_string_;
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

