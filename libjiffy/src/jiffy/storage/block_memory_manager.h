#ifndef JIFFY_BLOCK_MEMORY_MANAGER_H
#define JIFFY_BLOCK_MEMORY_MANAGER_H

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <new>

namespace jiffy {
namespace storage {

struct memory_block_overflow : std::bad_alloc {
  using std::bad_alloc::bad_alloc;
};

/**
 * @brief Memory allocator that tracks size internally.
 */
class block_memory_manager {
 public:
  /**
   * @brief Constructor.
   * @param capacity Maximum capacity of block.
   */
  explicit block_memory_manager(size_t capacity = 134217728);

  /**
   * @brief Allocate memory.
   * @param size Number of bytes to allocate.
   * @return Pointer to allocated memory, returns null if allocation fails.
   */
  void *mb_malloc(size_t size);

  /**
   * @brief Free memory.
   * @param ptr Pointer to memory allocation.
   */
  void mb_free(void *ptr);

  /**
   * @brief Free memory.
   * @param ptr Pointer to memory allocation.
   * @param size Number of bytes to free.
   */
  void mb_free(void *ptr, size_t size);

  /**
   * @brief Get capacity of memory block.
   * @return Capacity of memory block.
   */
  size_t mb_capacity() const;

  /**
   * @brief Get used number of bytes in memory block.
   * @return Used number of bytes in memory block.
   */
  size_t mb_used() const;

  /**
   * @brief Check if two block memory managers are the same.
   * @param other Instance of other block memory manager.
   * @return True if the instances are equal, false otherwise.
   */
  inline bool operator==(const block_memory_manager &other) {
    return this == &other;
  }

  /**
   * @brief Check if two block memory managers are not the same.
   * @param other Instance of other block memory manager.
   * @return True if the instances are unequal, false otherwise.
   */
  inline bool operator!=(const block_memory_manager &other) {
    return this != &other;
  }

 private:
  size_t capacity_;
  std::atomic<size_t> used_;
};

}
}

#endif //JIFFY_BLOCK_MEMORY_MANAGER_H
