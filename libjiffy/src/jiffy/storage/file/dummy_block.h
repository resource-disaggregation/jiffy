#ifndef JIFFY_DUMMY_BLOCK_H
#define JIFFY_DUMMY_BLOCK_H

#include <string>
#include <cstring>
#include <vector>
#include <map>
#include <iterator>
#include "jiffy/storage/block_memory_allocator.h"

namespace jiffy {
namespace storage {
/**
 * @brief Dummy_block class
 * This data structure only mainly blocks of memory without metadata
 * Handles read write across multiple blocks
 */
class dummy_block {
  typedef std::ptrdiff_t difference_type;
  typedef std::size_t size_type;
  typedef std::string value_type;
  typedef std::string *pointer;
  typedef std::string &reference;
  typedef const std::string &const_reference;
 public:
  /**
   * @brief Constructor
   */
  dummy_block() = default;

  /**
   * @brief Constructor
   * @param max_size Max size for dummy block
   * @param alloc Block memory allocator 
   */

  dummy_block(std::size_t max_size, block_memory_allocator<char> alloc);

  /**
   * @brief Deconstructor
   */

  ~dummy_block();

  /**
   * @brief Copy constructor
   * @param other Another dummy block
   */

  dummy_block(const dummy_block &other);

  /**
   * @brief Copy assignment operator
   * @param other Another dummy block
   * @return Dummy block
   */

  dummy_block &operator=(const dummy_block &other);

  /**
   * @brief Operator ==
   * @param other Another dummy block
   * @return Boolean, true if equal 
   */

  bool operator==(const dummy_block &other) const;

  /**
   * @brief Push new message at the end of the block
   * @param msg Message
   * @return Pair, a status boolean and the remain string
   */
  std::pair<bool, std::string> push_back(const std::string &msg);

  /**
   * @brief Read string at offset with given size
   * @param offset Read offset
   * @param size Size to read
   * @param Pair, a status boolean and the read string
   */

  const std::pair<bool, std::string> read(std::size_t offset, std::size_t size) const;

  /**
   * @brief Fetch total size of the block
   * @return Size
   */

  std::size_t size() const;


  /**
   * @brief Fetch capacity of the block
   * @return Capacity
   */

  std::size_t capacity();

  /**
   * @brief Clear the content of the block
   */

  void clear();

  /**
   * @brief Check if block is empty
   * @return Boolean, true if empty
   */
  bool empty() const;

  /**
   * @brief Fetch the maximum offset of the strings
   * @return Maximum offset of the strings
   */

  std::size_t max_offset() const;

  /**
   * @brief Fetch data pointer
   * @return Data pointer of the block
   */

  char* data() const;

 private:
  /* Block memory allocator */
  block_memory_allocator<char> alloc_;

  /* Maximum capacity */
  std::size_t max_{};

  /* Data pointer */
  char *data_{};

  /* Tail position */
  std::size_t tail_{};

  /* Boolean, true if the file is extended */
  bool extend_;
};

}
}

#endif //JIFFY_DUMMY_BLOCK_H