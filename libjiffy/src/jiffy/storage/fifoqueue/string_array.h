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

class string_array_iterator;
class string_array;
class const_string_array_iterator;

/**
 * @brief String array class
 *
 * This data structure store strings in "length | string" format
 * and supports storing big strings between different data blocks.
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
  static const int METADATA_LEN = 8;

  /**
   * @brief Constructor
   */
  string_array() = default;

  /**
   * @brief Constructor
   * @param max_size Max size for the string array
   * @param alloc Block memory allocator
   */
  string_array(std::size_t max_size, block_memory_allocator<char> alloc);

  /**
   * @brief Destructor
   */
  ~string_array();

  /**
   * @brief Copy constructor
   * @param other Another string array
   */
  string_array(const string_array &other);

  /**
   * @brief Copy assignment operator
   * @param other Another string array
   * @return String array
   */
  string_array &operator=(const string_array &other);

  /**
   * @brief Equal operator
   * @param other Another string array
   * @return Boolean, true if equal
   */
  bool operator==(const string_array &other) const;

  /**
   * @brief Push new message at the end of the array
   * @param item Message
   * @return Pair, a status boolean and the remain string
   */
  std::pair<bool, std::string> push_back(const std::string &item);

  /**
   * @brief Read string at offset
   * @param offset Read offset
   * @param Pair, a status boolean and the read string
   */
  const std::pair<bool, std::string> at(std::size_t offset) const;

  /**
   * @brief Find next string for the given offset string
   * @param offset Offset of the current string
   * @return Offset of the next string
   */
  std::size_t find_next(std::size_t offset) const;

  /**
   * @brief Fetch total size of the string array
   * @return Size
   */
  std::size_t size() const;

  /**
   * @brief Fetch last element offset in the partition
   * @return Last element offset
   */
  std::size_t last_element_offset() const;

  /**
   * @brief Fetch capacity of the string array
   * @return Capacity
   */
  std::size_t capacity();

  /**
   * @brief Clear the content of string array
   */
  void clear();

  /**
   * @brief Check if string array is empty
   * @return Boolean, true if empty
   */
  bool empty() const;

  /**
   * @brief Check if string array is full
   * @return Boolean, true if full
   */
  bool full() const;

  /**
   * @brief Fetch begin iterator
   * @return Begin iterator
   */
  iterator begin();

  /**
   * @brief Fetch end iterator
   * @return End iterator
   */
  iterator end();

  /**
   * @brief Fetch const begin iterator
   * @return Const begin iterator
   */
  const_iterator begin() const;

  /**
   * @brief Fetch const end iterator
   * @return Const end iterator
   */
  const_iterator end() const;

  /**
   * @brief Fetch the maximum offset of the strings
   * @return Maximum offset of the strings
   */
  std::size_t max_offset() const;

  /**
   * @brief Fetch the number of elements in the string array
   * @return Number of elements
   */
  std::size_t num_elements() const;

 private:
  /* Block memory allocator */
  block_memory_allocator<char> alloc_;

  /* Offset of the last element */
  std::size_t last_element_offset_;

  /* Maximum capacity */
  std::size_t max_{};

  /* Data pointer */
  char *data_{};

  /* Tail position */
  std::size_t tail_{};

  /* Bool for split string */
  bool split_string_;

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
  /**
   * @brief Constructor
   * @param impl String array reference
   * @param pos Position
   */
  string_array_iterator(string_array &impl, std::size_t pos);

  /**
   * @brief Operator *
   * @return Value
   */
  value_type operator*() const;

  /**
   * @brief Operator ++
   * @return string array iterator
   */
  const string_array_iterator operator++(int);

  /**
   * @brief Operator ==
   * @param other Another string array iterator
   * @return Boolean, true if equal
   */
  bool operator==(string_array_iterator other) const;

  /**
   * @brief Operator !=
   * @param other Another string array iterator
   * @return Boolean, true if not equal
   */
  bool operator!=(string_array_iterator other) const;

  /**
   * @brief Operator =
   * @param other Another string array iterator
   * @return string array iterator
   */
  string_array_iterator &operator=(const string_array_iterator &other);

 private:
  /* String array */
  string_array &impl_;

  /* Position */
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
  /**
   * @brief Constructor
   * @param impl String array reference
   * @param pos Position
   */
  const_string_array_iterator(const string_array &impl, std::size_t pos);

  /**
   * @brief Operator *
   * @return Value
   */
  value_type operator*() const;

  /**
   * @brief Operator ++
   * @return const string array iterator
   */
  const const_string_array_iterator operator++(int);

  /**
   * @brief Operator ==
   * @param other Another const string array iterator
   * @return Boolean, true if equal
   */
  bool operator==(const_string_array_iterator other) const;

  /**
   * @brief Operator !=
   * @param other Another const string array iterator
   * @return Boolean, true if not equal
   */
  bool operator!=(const_string_array_iterator other) const;

 private:
  /* String array */
  const string_array &impl_;

  /* Position */
  std::size_t pos_;
};

}
}

#endif //JIFFY_STRING_ARRAY_H
