#ifndef JIFFY_FILE_BLOCK_H
#define JIFFY_FILE_BLOCK_H

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
class file_block {
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
  file_block() = default;

  /**
   * @brief Constructor
   * @param max_size Max size for dummy block
   * @param alloc Block memory allocator 
   */

  file_block(std::size_t max_size, block_memory_allocator<char> alloc);

  /**
   * @brief Deconstructor
   */

  ~file_block();

  /**
   * @brief Copy constructor
   * @param other Another dummy block
   */

  file_block(const file_block &other);

  /**
   * @brief Copy assignment operator
   * @param other Another dummy block
   * @return Dummy block
   */

  file_block &operator=(const file_block &other);

  /**
   * @brief Operator ==
   * @param other Another dummy block
   * @return Boolean, true if equal 
   */

  bool operator==(const file_block &other) const;

  /**
   * @brief Write new message to the block
   * @param offset Offset
   * @param data Message
   * @return Pair, a status boolean and the remain string
   */
  std::pair<bool, std::string> write(const std::string &data, std::size_t offset);

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
   * @brief Clear the content of the block
   */

  void clear();

  /**
   * @brief Fetch data pointer
   * @return Data pointer of the block
   */

  char *data() const;

 private:
  /* Block memory allocator */
  block_memory_allocator<char> alloc_;

  /* Maximum capacity */
  std::size_t max_{};

  /* Data pointer */
  char *data_{};
};

}
}

#endif //JIFFY_FILE_BLOCK_H