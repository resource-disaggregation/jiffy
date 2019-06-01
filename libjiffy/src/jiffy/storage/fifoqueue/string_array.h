#ifndef JIFFY_STRING_ARRAY_H
#define JIFFY_STRING_ARRAY_H

#include <string>
#include <cstring>
#include <vector>
#include <map>
#include <iterator>
#include "jiffy/storage/block_memory_allocator.h"

namespace jiffy {
namespace storage {

const int metadata_length = 8; // C++ std::size_t is 8 bytes
class string_array_iterator;
class string_array;
class const_string_array_iterator;
/**
 * @brief String array class
 */
class string_array {
  friend class string_array_iterator;
  friend class const_string_array_iterator;
  typedef string_array_iterator iterator;
  typedef const_string_array_iterator const_iterator;
  typedef std::ptrdiff_t difference_type;
  typedef std::size_t size_type;
  typedef std::string value_type;
  typedef std::string *pointer;
  typedef std::string &reference;
  typedef const std::string &const_reference;
 public:
  string_array() = default;
  string_array(std::size_t max_size, block_memory_allocator<char> alloc);
  ~string_array();
  string_array(const string_array &other);
  string_array &operator=(const string_array &other);
  bool operator==(const string_array &other) const;
  std::pair<bool, std::string> push_back(const std::string &msg);
  const std::pair<bool, std::string> at(std::size_t offset) const;
  std::size_t find_next(std::size_t offset) const;
  std::size_t size() const;
  std::size_t capacity();
  bool split_last_string();
  void clear();
  bool empty() const;
  iterator begin();
  iterator end();
  const_iterator begin() const;
  const_iterator end() const;
  std::size_t max_offset() const;

 private:
  std::atomic<bool> split_string_{};
  block_memory_allocator<char> alloc_;
  std::size_t last_element_offset_;
  std::size_t max_{};
  char *data_{};
  std::size_t tail_{};
};

/**
 * Iterator for string array.
 */

class string_array_iterator {
  typedef typename string_array::value_type value_type;
  typedef typename string_array::difference_type difference_type;
  typedef typename string_array::pointer pointer;
  typedef typename string_array::reference reference;
  typedef typename string_array::const_reference const_reference;
 public:

  string_array_iterator(string_array &impl, std::size_t pos);
  value_type operator*() const;

  const string_array_iterator operator++(int);

  bool operator==(string_array_iterator other) const;

  bool operator!=(string_array_iterator other) const;

  string_array_iterator &operator=(const string_array_iterator &other);

 private:
  string_array &impl_;
  std::size_t pos_;
};
/**
 * Const iterator for string array.
 */

class const_string_array_iterator {
  typedef typename string_array::value_type value_type;
  typedef typename string_array::difference_type difference_type;
  typedef typename string_array::pointer pointer;
  typedef typename string_array::reference reference;
  typedef typename string_array::const_reference const_reference;
 public:

  const_string_array_iterator(const string_array &impl, std::size_t pos);
  value_type operator*() const;

  const const_string_array_iterator operator++(int);

  bool operator==(const_string_array_iterator other) const;

  bool operator!=(const_string_array_iterator other) const;

 private:
  const string_array &impl_;
  std::size_t pos_;
};

}
}

#endif //JIFFY_STRING_ARRAY_H