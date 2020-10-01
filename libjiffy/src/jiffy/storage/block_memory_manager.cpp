// #include <jemalloc/jemalloc.h>
#include <memkind.h>
#include <new>
#include <iostream>
#include "block_memory_manager.h"
#include "jiffy/utils/logger.h"
using namespace jiffy::utils;

namespace jiffy {
namespace storage {

block_memory_manager::block_memory_manager(size_t capacity) : capacity_(capacity), used_(0) {}

void *block_memory_manager::mb_malloc(size_t size) {
  if (used_.load() > capacity_) {
    return nullptr;
  }
  // auto ptr = mallocx(size, 0);
  auto ptr = memkind_malloc(MEMKIND_DEFAULT, size);
  used_ += size;
  return ptr;
}

void block_memory_manager::mb_free(void *ptr) {
  // auto size = sallocx(ptr, 0);
  
  // used_ -= size;
  auto size = memkind_malloc_usable_size(MEMKIND_DEFAULT, ptr);
  memkind_free(MEMKIND_DEFAULT, ptr);
  std::cout<<"used_size="<<size<<std::endl;
  used_ -= size;

}

void block_memory_manager::mb_free(void *ptr, size_t size) {
  memkind_free(MEMKIND_DEFAULT, ptr);
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