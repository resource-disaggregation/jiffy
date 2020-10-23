#include <memkind.h>
#include <new>
#include <iostream>
#include "block_memory_manager.h"
#include "jiffy/utils/logger.h"
#include <limits.h>
using namespace jiffy::utils;

namespace jiffy {
namespace storage {

block_memory_manager::block_memory_manager(size_t capacity, const std::string memory_mode, struct memkind* pmem_kind) : capacity_(capacity), used_(0), memory_mode_(memory_mode), pmem_kind_(pmem_kind) {}

void *block_memory_manager::mb_malloc(size_t size) {
  if (used_.load() > capacity_) {
    return nullptr;
  }
  if (memory_mode_ == "PMEM"){
    auto ptr = memkind_malloc(pmem_kind_, size);
    used_ += size;
    return ptr;
  }
  else if (memory_mode_ == "DRAM"){
    auto ptr = memkind_malloc(MEMKIND_DEFAULT, size);
    used_ += size;
    return ptr;
  }
}

void block_memory_manager::mb_free(void *ptr) {
  if (memory_mode_ == "PMEM"){
    auto size = memkind_malloc_usable_size(pmem_kind_, ptr);
    memkind_free(pmem_kind_, ptr);
    used_ -= size;
  }
  else if (memory_mode_ == "DRAM"){
    auto size = memkind_malloc_usable_size(MEMKIND_DEFAULT, ptr);
    memkind_free(MEMKIND_DEFAULT, ptr);
    used_ -= size;
  }
}

void block_memory_manager::mb_free(void *ptr, size_t size) {
  std::cout<<"enter free\n";
  if (memory_mode_ == "PMEM"){
    memkind_free(pmem_kind_, ptr);
    used_ -= size;
  }
  else if (memory_mode_ == "DRAM"){
    memkind_free(MEMKIND_DEFAULT, ptr);
    used_ -= size;
  }
}

size_t block_memory_manager::mb_capacity() const {
  return capacity_;
}

size_t block_memory_manager::mb_used() const {
  return used_.load();
}

}
}


// #include <memkind.h>
// #include <new>
// #include <iostream>
// #include "block_memory_manager.h"
// #include "jiffy/utils/logger.h"
// #include <limits.h>
// using namespace jiffy::utils;

// namespace jiffy {
// namespace storage {

// block_memory_manager::block_memory_manager(size_t capacity, struct memkind* pmem_kind) : capacity_(capacity), used_(0), pmem_kind_(pmem_kind) {}


// void *block_memory_manager::mb_malloc(size_t size) {
//   if (used_.load() > capacity_) {
//     return nullptr;
//   }
//   // auto ptr = mallocx(size, 0);
//   auto ptr = memkind_malloc(MEMKIND_DEFAULT, size);
//   used_ += size;
//   return ptr;
// }

// void block_memory_manager::mb_free(void *ptr) {
//   // auto size = sallocx(ptr, 0);

//   // used_ -= size;
//   auto size = memkind_malloc_usable_size(MEMKIND_DEFAULT, ptr);
//   memkind_free(MEMKIND_DEFAULT, ptr);
//   used_ -= size;

// }

// void block_memory_manager::mb_free(void *ptr, size_t size) {
//   memkind_free(MEMKIND_DEFAULT, ptr);
//   used_ -= size;
// }

// size_t block_memory_manager::mb_capacity() const {
//   return capacity_;
// }

// size_t block_memory_manager::mb_used() const {
//   return used_.load();
// }

// }
// }
