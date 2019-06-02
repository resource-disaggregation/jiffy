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
 */
class dummy_block {
  typedef std::ptrdiff_t difference_type;
  typedef std::size_t size_type;
  typedef std::string value_type;
  typedef std::string *pointer;
  typedef std::string &reference;
  typedef const std::string &const_reference;
 public:
  dummy_block() = default;
  dummy_block(std::size_t max_size, block_memory_allocator<char> alloc);
  ~dummy_block();
  dummy_block(const dummy_block &other);
  dummy_block &operator=(const dummy_block &other);
  bool operator==(const dummy_block &other) const;
  std::pair<bool, std::string> push_back(const std::string &msg);
  const std::pair<bool, std::string> read(std::size_t offset, std::size_t size) const;
  std::size_t size() const;
  std::size_t capacity();
  void clear();
  bool empty() const;
  std::size_t max_offset() const;
  char* data() const;
 private:
  block_memory_allocator<char> alloc_;
  std::size_t max_{};
  char *data_{};
  std::size_t tail_{};
  bool extend_;
};

}
}

#endif //JIFFY_DUMMY_BLOCK_H