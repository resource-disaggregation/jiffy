#include <jemalloc/jemalloc.h>
#include <new>
#include "block_memory_manager.h"

namespace jiffy {
namespace storage {

block_memory_manager::block_memory_manager(size_t capacity) : capacity_(capacity), used_(0) {}

void *block_memory_manager::mb_malloc(size_t size) {
  if (used_.load() + size > capacity_) {
    return nullptr;
  }
  auto ptr = mallocx(size, 0);
  used_ += sallocx(ptr, 0);
  return ptr;
}

void block_memory_manager::mb_free(void *ptr) {
  free(ptr);
  used_ -= sallocx(ptr, 0);
}

size_t block_memory_manager::mb_capacity() const {
  return capacity_;
}

size_t block_memory_manager::mb_used() const {
  return used_.load();
}

}
}