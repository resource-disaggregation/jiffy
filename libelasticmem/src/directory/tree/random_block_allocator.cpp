#include <random>
#include <algorithm>
#include <iostream>
#include "random_block_allocator.h"

namespace elasticmem {
namespace directory {

random_block_allocator::random_block_allocator(const std::vector<std::string> &blocks) : free_blocks_(blocks) {}

std::string random_block_allocator::allocate(const std::string &) {
  std::unique_lock<std::shared_mutex> lock(mtx_);
  if (free_blocks_.empty()) {
    throw std::out_of_range("No free blocks to allocate from");
  }
  static thread_local std::mt19937 generator;
  std::uniform_int_distribution<std::size_t> distribution(0, free_blocks_.size() - 1);
  std::size_t idx = distribution(generator);
  allocated_blocks_.push_back(free_blocks_.at(idx));
  free_blocks_.erase(free_blocks_.begin() + idx);
  return allocated_blocks_.back();
}

void random_block_allocator::free(const std::string &block_name) {
  std::unique_lock<std::shared_mutex> lock(mtx_);
  auto it = std::find(allocated_blocks_.begin(), allocated_blocks_.end(), block_name);
  if (it == allocated_blocks_.end()) {
    throw std::out_of_range("No such allocated block: " + block_name);
  }
  auto block = *it;
  allocated_blocks_.erase(it);
  free_blocks_.push_back(block);
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