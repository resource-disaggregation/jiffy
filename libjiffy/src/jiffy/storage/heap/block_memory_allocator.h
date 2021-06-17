#ifndef JIFFY_BLOCK_MEMORY_ALLOCATOR_H
#define JIFFY_BLOCK_MEMORY_ALLOCATOR_H

#include <cstddef>
#include <stdexcept>
#include <type_traits>
#include <memory>
#include "block_memory_manager.h"

namespace jiffy {
namespace storage {

template<typename T>
class block_memory_allocator {
 public:
  template<typename U> friend
  class block_memory_allocator;

  // type definitions
  typedef T value_type;
  typedef T *pointer;
  typedef const T *const_pointer;
  typedef T &reference;
  typedef const T &const_reference;
  typedef std::size_t size_type;
  typedef std::ptrdiff_t difference_type;

  using propagate_on_container_copy_assignment = std::true_type;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;

  // rebind allocator to type U
  template<class U>
  struct rebind {
    typedef block_memory_allocator<U> other;
  };

  // return address of values
  pointer address(reference value) const {
    return &value;
  }

  const_pointer address(const_reference value) const {
    return &value;
  }

  block_memory_allocator() : manager_(nullptr) {};

  explicit block_memory_allocator(block_memory_manager *manager)
      : manager_(manager) {}

  template<class U>
  explicit block_memory_allocator(const block_memory_allocator<U> &other)
      : manager_(other.manager_) {}

  ~block_memory_allocator() = default;

  // return maximum number of elements that can be allocated
  size_type max_size() const {
    return manager_->mb_capacity() / sizeof(T);
  }

  // allocate but don't initialize num elements of type T
  pointer allocate(size_type num, const void * = 0) {
    size_t requested_bytes = num * sizeof(T);
    if (requested_bytes == 0) return nullptr;
    auto p = static_cast<pointer>(manager_->mb_malloc(num * sizeof(T)));
    if (p == nullptr) {
      if (manager_->mb_used() + requested_bytes > manager_->mb_capacity()) {
        throw memory_block_overflow();
      } else {
        throw std::bad_alloc();
      }
    }
    return p;
  }

  // initialize elements of allocated storage p with value value
  void construct(pointer p, const T &value) {
    // initialize memory with placement new
    new((void *) p)T(value);
  }

  template<class U, class... Args>
  void construct(U *p, Args &&... args) {
    ::new((void *) p) U(std::forward<Args>(args)...);
  }

  // destroy elements of initialized storage p
  void destroy(pointer p) {
    // destroy objects by calling their destructor
    p->~T();
  }

  // deallocate storage p of deleted elements
  void deallocate(pointer p, size_type size) {
    if (p == nullptr)
      return;
    manager_->mb_free(p, size);
  }

  template<typename U>
  bool operator==(block_memory_allocator<U> const &rhs) const {
    return manager_ == rhs.manager_;
  }

  template<typename U>
  bool operator!=(block_memory_allocator<U> const &rhs) const {
    return manager_ != rhs.manager_;
  }

 private:
  block_memory_manager *manager_;
};

}
}

#endif //JIFFY_BLOCK_MEMORY_ALLOCATOR_H
