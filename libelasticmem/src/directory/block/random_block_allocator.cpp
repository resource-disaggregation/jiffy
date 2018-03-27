#include <random>
#include <algorithm>
#include <iostream>
#include "random_block_allocator.h"
#include "../../utils/rand_utils.h"

namespace elasticmem {
namespace directory {

random_block_allocator::random_block_allocator(const std::vector<std::string> &blocks) : free_blocks_(blocks) {}

std::vector<std::string> random_block_allocator::allocate(std::size_t count, const std::vector<std::string> &) {
  std::unique_lock<std::shared_mutex> lock(mtx_);
  if (count > free_blocks_.size()) {
    throw std::out_of_range("Insufficient free blocks to allocate from");
  }
  std::vector<std::string> allocated;
  for (std::size_t i = 0; i < count; i++) {
    std::size_t idx = utils::rand_utils::rand_uint64(free_blocks_.size() - 1);
    allocated.push_back(free_blocks_.at(idx));
    allocated_blocks_.push_back(free_blocks_.at(idx));
    free_blocks_.erase(free_blocks_.begin() + idx);
  }
  return allocated;
}

void random_block_allocator::free(const std::vector<std::string> &blocks) {
  std::unique_lock<std::shared_mutex> lock(mtx_);
  std::vector<std::string> not_freed;
  for (auto& block_name: blocks) {
    auto it = std::find(allocated_blocks_.begin(), allocated_blocks_.end(), block_name);
    if (it == allocated_blocks_.end()) {
      not_freed.push_back(block_name);
      continue;
    }
    auto block = *it;
    allocated_blocks_.erase(it);
    free_blocks_.push_back(block);
  }
  if (!not_freed.empty()) {
    std::string not_freed_string;
    for (const auto &b: not_freed) {
      not_freed_string += (b + "; ");
    }
    throw std::out_of_range("Could not free these blocks because they have not been allocated: " + not_freed_string);
  }
}

void random_block_allocator::add_blocks(const std::vector<std::string> &block_names) {
  std::unique_lock<std::shared_mutex> lock(mtx_);
  free_blocks_.insert(free_blocks_.end(), block_names.begin(), block_names.end());
}

void random_block_allocator::remove_blocks(const std::vector<std::string> &block_names) {
  std::unique_lock<std::shared_mutex> lock(mtx_);
  for (const auto &block_name: block_names) {
    auto it = std::find(free_blocks_.begin(), free_blocks_.end(), block_name);
    if (it == free_blocks_.end()) {
      throw std::out_of_range("Trying to remove an allocated block: " + block_name);
    }
    free_blocks_.erase(it);
  }
}

std::size_t random_block_allocator::num_free_blocks() {
  std::shared_lock<std::shared_mutex> lock(mtx_);
  return free_blocks_.size();
}

std::size_t random_block_allocator::num_allocated_blocks() {
  std::shared_lock<std::shared_mutex> lock(mtx_);
  return allocated_blocks_.size();
}

std::size_t random_block_allocator::num_total_blocks() {
  std::shared_lock<std::shared_mutex> lock(mtx_);
  return free_blocks_.size() + allocated_blocks_.size();
}

}
}