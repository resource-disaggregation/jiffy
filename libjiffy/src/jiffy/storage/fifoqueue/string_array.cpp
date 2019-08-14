#include "string_array.h"

namespace jiffy {
namespace storage {

string_array::string_array(std::size_t capacity, block_memory_allocator<char> alloc)
    : head_(0), tail_(0), size_(0), capacity_(capacity), alloc_(alloc) {
  data_ = reinterpret_cast<uint8_t *>(alloc_.allocate(capacity_));
}

string_array::~string_array() {
  alloc_.deallocate(reinterpret_cast<block_memory_allocator<char>::pointer>(data_), capacity_);
}

string_array::string_array(const string_array &other) {
  alloc_ = other.alloc_;
  capacity_ = other.capacity_;
  data_ = other.data_;
  tail_ = other.tail_;
}

string_array &string_array::operator=(const string_array &other) = default;

bool string_array::operator==(const string_array &other) const {
  return data_ == other.data_ && tail_ == other.tail_ && alloc_ == other.alloc_ && capacity_ == other.capacity_;
}

void string_array::write(std::size_t offset, const std::string &item) {
  // Write size
  uint32_t sz = item.size();
  data_[offset % capacity_] = sz;
  data_[(offset + 1) % capacity_] = sz >> 8u;
  data_[(offset + 2) % capacity_] = sz >> 16u;
  data_[(offset + 3) % capacity_] = sz >> 24u;

  // Write data
  auto data_offset = (offset + METADATA_LEN) % capacity_;
  if (data_offset + sz <= capacity_) {
    std::memcpy(data_ + data_offset, item.data(), sz);
  } else {
    size_t sz1 = capacity_ - data_offset;
    std::memcpy(data_ + data_offset, item.data(), sz1);
    size_t sz2 = sz - sz1;
    std::memcpy(data_, item.data() + sz1, sz2);
  }
}

std::string string_array::read(std::size_t offset) const {
  // Read size
  uint32_t u0 = data_[offset % capacity_];
  uint32_t u1 = data_[(offset + 1) % capacity_];
  uint32_t u2 = data_[(offset + 2) % capacity_];
  uint32_t u3 = data_[(offset + 3) % capacity_];
  uint32_t sz = u0 | (u1 << 8u) | (u2 << 16u) | (u3 << 24u);

  // Read data
  auto data_offset = (offset + METADATA_LEN) % capacity_;
  std::string buf(sz, '\0');
  if (data_offset + sz <= capacity_) {
    std::memcpy(&buf[0], data_ + data_offset, sz);
  } else {
    size_t sz1 = capacity_ - data_offset;
    std::memcpy(&buf[0], data_ + data_offset, sz1);
    size_t sz2 = sz - sz1;
    std::memcpy(&buf[0] + sz1, data_, sz2);
  }
  return buf;
}

void string_array::push_back(const std::string &item) {
  if (!enqueue(item)) {
    throw std::out_of_range("!full");
  }
}

bool string_array::enqueue(const std::string &item) {
  if (size_ + METADATA_LEN + item.size() > capacity_) {
    // The buffer cannot accommodate the item
    return false;
  }

  write(tail_, item);
  tail_ = (tail_ + METADATA_LEN + item.size()) % capacity_;
  size_ += (METADATA_LEN + item.size());
  return true;
}

bool string_array::dequeue(std::string &item) {
  if (size_ == 0) {
    // The buffer is empty
    return false;
  }
  item = read(head_);
  head_ = (head_ + METADATA_LEN + item.size()) % capacity_;
  size_ -= (METADATA_LEN + item.size());
  return true;
}

std::size_t string_array::find_next(std::size_t offset) const {
  // Read size
  uint32_t u0 = data_[offset % capacity_];
  uint32_t u1 = data_[(offset + 1) % capacity_];
  uint32_t u2 = data_[(offset + 2) % capacity_];
  uint32_t u3 = data_[(offset + 3) % capacity_];
  uint32_t sz = u0 | (u1 << 8u) | (u2 << 16u) | (u3 << 24u);
  return offset + sz + METADATA_LEN;
}

std::size_t string_array::size() const {
  return size_;
}

std::size_t string_array::capacity() const {
  return capacity_;
}

void string_array::clear() {
  tail_ = 0;
  head_ = 0;
  size_ = 0;
}

bool string_array::empty() const {
  return head_ == tail_;
}

string_array::iterator string_array::begin() {
  return string_array::iterator(*this, 0);
}

string_array::iterator string_array::end() {
  return string_array::iterator(*this, capacity_);
}

string_array::const_iterator string_array::begin() const {
  return {*this, 0};
}

string_array::const_iterator string_array::end() const {
  return {*this, capacity_};
}

string_array_iterator::string_array_iterator(string_array &impl, std::size_t pos)
    : impl_(impl),
      pos_(pos) {}

string_array_iterator::value_type string_array_iterator::operator*() const {
  return impl_.read(pos_);
}

const string_array_iterator string_array_iterator::operator++(int) {
  if (pos_ != impl_.tail_)
    pos_ = impl_.find_next(pos_);
  else
    pos_ = impl_.capacity_;
  return *this;
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
  return impl_.read(pos_);
}

const const_string_array_iterator const_string_array_iterator::operator++(int) {
  if (pos_ != impl_.tail_)
    pos_ = impl_.find_next(pos_);
  else
    pos_ = impl_.capacity_;
  return *this;
}

bool const_string_array_iterator::operator==(const_string_array_iterator other) const {
  return (impl_ == other.impl_) && (pos_ == other.pos_);
}

bool const_string_array_iterator::operator!=(const_string_array_iterator other) const {
  return !(*this == other);
}

}
}

