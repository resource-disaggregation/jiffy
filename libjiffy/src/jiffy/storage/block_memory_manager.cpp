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



// #include <jemalloc/jemalloc.h>
#include <memkind.h>
#include <new>
#include <iostream>
#include "block_memory_manager.h"
#include "jiffy/utils/logger.h"
#include <limits.h>
using namespace jiffy::utils;

namespace jiffy {
namespace storage {

block_memory_manager::block_memory_manager(size_t capacity) : capacity_(capacity), used_(0) {
  char path[PATH_MAX] = "/media/pmem0/shijie/";
  err = memkind_create_pmem(path,0,&pmem_kind);
}

void *block_memory_manager::mb_malloc(size_t size) {
  std::cout<<"enter mb_malloc"<<std::endl;
  if (used_.load() > capacity_) {
    return nullptr;
  }
  std::cout<<"before memkind malloc, used="<<used_<<", size="<<size<<std::endl;
  // auto ptr = mallocx(size, 0);
  auto ptr = memkind_malloc(pmem_kind, size);
  std::cout<<"after memkind malloc"<<std::endl;
  used_ += size;
  return ptr;
}

void block_memory_manager::mb_free(void *ptr) {
  // auto size = sallocx(ptr, 0);

  // used_ -= size;
  auto size = memkind_malloc_usable_size(pmem_kind, ptr);
  memkind_free(pmem_kind, ptr);
  used_ -= size;

}

void block_memory_manager::mb_free(void *ptr, size_t size) {
  memkind_free(pmem_kind, ptr);
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
