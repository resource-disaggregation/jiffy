#ifdef MEMKIND_IN_USE
  #include <memkind.h>
#else
  #include <jemalloc/jemalloc.h>
#endif
#include <new>
#include "block_memory_manager.h"
#include "jiffy/utils/logger.h"
using namespace jiffy::utils;

namespace jiffy {
namespace storage {

block_memory_manager::block_memory_manager(size_t capacity, const std::string memory_mode, void* mem_kind)
    : capacity_(capacity), used_(0), memory_mode_(memory_mode), mem_kind_(mem_kind) {}

void *block_memory_manager::mb_malloc(size_t size) {
  if (used_.load() > capacity_) {
    return nullptr;
  }
  #ifdef MEMKIND_IN_USE
    if (memory_mode_ == "DRAM") {
      mem_kind_ = MEMKIND_DEFAULT;
    }
    auto ptr = memkind_malloc((struct memkind*)mem_kind_, size);
  #else
    auto ptr = mallocx(size, 0);
  #endif 
  used_ += size;
  return ptr;
}

void block_memory_manager::mb_free(void *ptr) {
  #ifdef MEMKIND_IN_USE
    auto size = memkind_malloc_usable_size((struct memkind*)mem_kind_, ptr);
    memkind_free((struct memkind*)mem_kind_, ptr);
  #else 
    auto size = sallocx(ptr, 0);
    free(ptr);
  #endif
  used_ -= size;
}

void block_memory_manager::mb_free(void *ptr, size_t size) {
  #ifdef MEMKIND_IN_USE
    memkind_free((struct memkind*)mem_kind_, ptr);
  #else
    free(ptr);
  #endif
  used_ -= size;
}

size_t block_memory_manager::mb_capacity() const {
  return capacity_;
}

size_t block_memory_manager::mb_used() const {
  return used_.load();
}

}
}