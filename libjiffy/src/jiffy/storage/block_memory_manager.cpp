#include <memkind.h>
#include <new>
#include "block_memory_manager.h"
#include "jiffy/utils/logger.h"
using namespace jiffy::utils;

namespace jiffy {
namespace storage {

block_memory_manager::block_memory_manager(size_t capacity, const std::string memory_mode, struct memkind* pmem_kind) : capacity_(capacity), used_(0), memory_mode_(memory_mode), pmem_kind_(pmem_kind) {}

void *block_memory_manager::mb_malloc(size_t size) {
  if (used_.load() > capacity_) {
    return nullptr;
  }
  if (memory_mode_ == "DRAM"){
    pmem_kind_ = MEMKIND_DEFAULT;
  }
  auto ptr = memkind_malloc(pmem_kind_, size);
  used_ += size;
  return ptr;
}

void block_memory_manager::mb_free(void *ptr) {
  auto size = memkind_malloc_usable_size(pmem_kind_, ptr);
  memkind_free(pmem_kind_, ptr);
  used_ -= size;
}

void block_memory_manager::mb_free(void *ptr, size_t size) {
  memkind_free(pmem_kind_, ptr);
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