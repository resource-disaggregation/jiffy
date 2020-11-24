#include <random>
#include <algorithm>
#include <iostream>
#include "random_block_allocator.h"
#include "../../utils/rand_utils.h"

namespace jiffy {
namespace directory {

using namespace utils;

std::vector<std::string> random_block_allocator::allocate(std::size_t count, const std::vector<std::string> &, const std::string &/*tenant_id*/) {
  // LOG(log_level::info) << "Allocation tenant_id: " << tenant_id;
  std::unique_lock<std::mutex> lock(mtx_);
  if (count > free_blocks_.size()) {
    throw std::out_of_range(
        "Insufficient free blocks to allocate from (requested: " + std::to_string(count) + ", have: "
            + std::to_string(free_blocks_.size()));
  }
  std::vector<std::string> blocks;
  std::set<std::string> prefixes;

  // Find blocks with different prefixes
  size_t blocks_considered = 0;
  auto idx = static_cast<int64_t>(free_blocks_.size() - 1);
  auto it = std::next(free_blocks_.begin(), utils::rand_utils::rand_int64(idx));
  while (blocks_considered < free_blocks_.size() && blocks.size() < count) {
    auto block_prefix = prefix(*it);
    if (prefixes.find(block_prefix) == prefixes.end()) {
      blocks.push_back(*it);
      prefixes.insert(block_prefix);
    }
    if (++it == free_blocks_.end()) {
      it = free_blocks_.begin();
    }
    ++blocks_considered;
  }

  if (blocks.size() != count) {
    throw std::out_of_range("Could not find free blocks with distinct prefixes");
  }

  for (const auto &block: blocks) {
    free_blocks_.erase(block);
    allocated_blocks_.insert(block);
  }

  return blocks;
}

void random_block_allocator::free(const std::vector<std::string> &blocks, const std::string &/*tenant_id*/) {
  std::unique_lock<std::mutex> lock(mtx_);
  std::vector<std::string> not_freed;
  for (auto &block_name: blocks) {
    auto it = allocated_blocks_.find(block_name);
    if (it == allocated_blocks_.end()) {
      not_freed.push_back(block_name);
      continue;
    }
    free_blocks_.insert(*it);
    allocated_blocks_.erase(it);
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
  std::unique_lock<std::mutex> lock(mtx_);
  free_blocks_.insert(block_names.begin(), block_names.end());
}

void random_block_allocator::remove_blocks(const std::vector<std::string> &block_names) {
  std::unique_lock<std::mutex> lock(mtx_);
  for (const auto &block_name: block_names) {
    auto it = std::find(free_blocks_.begin(), free_blocks_.end(), block_name);
    if (it == free_blocks_.end()) {
      throw std::out_of_range("Trying to remove an allocated block: " + block_name);
    }
    free_blocks_.erase(it);
  }
}

std::size_t random_block_allocator::num_free_blocks() {
  std::unique_lock<std::mutex> lock(mtx_);
  return free_blocks_.size();
}

std::size_t random_block_allocator::num_allocated_blocks() {
  std::unique_lock<std::mutex> lock(mtx_);
  return allocated_blocks_.size();
}

std::size_t random_block_allocator::num_total_blocks() {
  std::unique_lock<std::mutex> lock(mtx_);
  return free_blocks_.size() + allocated_blocks_.size();
}

}
}
