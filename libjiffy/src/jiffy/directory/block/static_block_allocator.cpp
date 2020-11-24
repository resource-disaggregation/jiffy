#include <random>
#include <algorithm>
#include <iostream>
#include <cassert>
#include "static_block_allocator.h"

namespace jiffy {
namespace directory {

using namespace utils;

static_block_allocator::static_block_allocator(uint32_t num_tenants) {
    num_tenants_ = num_tenants;
    total_blocks_ = 0;
}

std::vector<std::string> static_block_allocator::allocate(std::size_t count, const std::vector<std::string> &, const std::string &tenant_id) {
  LOG(log_level::info) << "Allocation request for tenant_id: " << tenant_id;
  if(count > 1) {
      throw std::logic_error("Multi-block allocation support not implemented");
  }
  std::unique_lock<std::mutex> lock(mtx_);

  auto my = allocated_blocks_.find(tenant_id);
  if(my == allocated_blocks_.end())
  {
    // First time seeing this tenant, register
    register_tenant(tenant_id);
    my = allocated_blocks_.find(tenant_id);
    assert(my != allocated_blocks_.end());
  }

  std::size_t fair_share = total_blocks_ / num_tenants_;

  if(my->second.size() >= fair_share) {
    throw std::out_of_range("Could not find free blocks");
  }

  assert(free_blocks_.size() > 0);

  // Pick random block
  std::vector<std::string> blocks;
  auto idx = static_cast<int64_t>(free_blocks_.size() - 1);
  auto block_it = std::next(free_blocks_.begin(), utils::rand_utils::rand_int64(idx));
  
  my->second.insert(*block_it);
  blocks.push_back(*block_it);
  free_blocks_.erase(block_it);

  return blocks;
}

void static_block_allocator::free(const std::vector<std::string> &blocks, const std::string &tenant_id) {
  LOG(log_level::info) << "Free request for tenant_id: " << tenant_id;
  std::unique_lock<std::mutex> lock(mtx_);
  std::vector<std::string> not_freed;
  auto my = allocated_blocks_.find(tenant_id);
  if(my == allocated_blocks_.end())
  {
    throw std::logic_error("Unknown tenant");
  }
  for (auto &block_name: blocks) { 
    auto jt = my->second.find(block_name);
    if (jt == my->second.end()) {
      not_freed.push_back(block_name);
      continue;
    }
    free_blocks_.insert(*jt);
    my->second.erase(jt);
  }

  if (!not_freed.empty()) {
    std::string not_freed_string;
    for (const auto &b: not_freed) {
      not_freed_string += (b + "; ");
    }
    throw std::out_of_range("Could not free these blocks because they have not been allocated: " + not_freed_string);
  }
}

void static_block_allocator::add_blocks(const std::vector<std::string> &block_names) {
  std::unique_lock<std::mutex> lock(mtx_);
  total_blocks_ += block_names.size();
  free_blocks_.insert(block_names.begin(), block_names.end());
}

void static_block_allocator::remove_blocks(const std::vector<std::string> &block_names) {
  std::unique_lock<std::mutex> lock(mtx_);
  for (const auto &block_name: block_names) {
    auto it = std::find(free_blocks_.begin(), free_blocks_.end(), block_name);
    if (it == free_blocks_.end()) {
      throw std::out_of_range("Trying to remove an allocated block: " + block_name);
    }
    total_blocks_ -= 1;
    free_blocks_.erase(it);
  }
}

std::size_t static_block_allocator::num_free_blocks() {
  std::unique_lock<std::mutex> lock(mtx_);
  return free_blocks_.size();
}

// Needs to be called with lock
std::size_t static_block_allocator::num_allocated_blocks_unsafe()
{
  std::size_t res = 0;
  for(auto &it: allocated_blocks_)
  {
    res += it.second.size();
  }
  return res;
}

std::size_t static_block_allocator::num_allocated_blocks() {
  std::unique_lock<std::mutex> lock(mtx_);
  return num_allocated_blocks_unsafe();
  
}

std::size_t static_block_allocator::num_total_blocks() {
  std::unique_lock<std::mutex> lock(mtx_);
  return total_blocks_;
}


// Must be called with lock
void static_block_allocator::register_tenant(std::string tenant_id) {
  if(allocated_blocks_.size() >= num_tenants_) {
      throw std::logic_error("More than specified number of tenants");
  }  
  auto & tenant_blocks = allocated_blocks_[tenant_id];
  tenant_blocks.clear();
}

}
}







