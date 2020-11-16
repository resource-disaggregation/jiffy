#include <random>
#include <algorithm>
#include <iostream>
#include <cassert>
#include "maxmin_block_allocator.h"

namespace jiffy {
namespace directory {

using namespace utils;

std::vector<std::string> maxmin_block_allocator::allocate(std::size_t count, const std::vector<std::string> &, const std::string &tenant_id) {
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

  if(free_blocks_.size() == 0)
  {
    // Pick tenant with maximum allocation
    std::size_t max_allocation = 0;
    std::string max_tenant = "$none$";
    for(auto &jt : allocated_blocks_) {
      if(jt.second.size() > max_allocation) {
        max_allocation = jt.second.size();
        max_tenant = jt.first;
      }
    }

    if(max_tenant != "$none$" && max_tenant != my->first && max_allocation > my->second.size())
    {
      // Takeaway block from max_tenant
      // TODO: Picking random block for now. Better policy?
      auto idx = static_cast<int64_t>(allocated_blocks_[max_tenant].size() - 1);
      auto block_it = std::next(allocated_blocks_[max_tenant].begin(), utils::rand_utils::rand_int64(idx));
      assert(block_to_tenant_.erase(*block_it) == 1);
      free_blocks_.insert(*block_it);
      allocated_blocks_[max_tenant].erase(block_it);
    }
  }

  if(free_blocks_.size() == 0)
  {
    throw std::out_of_range("Could not find free blocks");
  }

  // Pick random block
  std::vector<std::string> blocks;
  auto idx = static_cast<int64_t>(free_blocks_.size() - 1);
  auto block_it = std::next(free_blocks_.begin(), utils::rand_utils::rand_int64(idx));
  my->second.insert(*block_it);
  block_to_tenant_[*block_it] = my->first;
  blocks.push_back(*block_it);
  free_blocks_.erase(block_it);

  return blocks;
}

void maxmin_block_allocator::free(const std::vector<std::string> &blocks, const std::string &/*tenant_id*/) {
  std::unique_lock<std::mutex> lock(mtx_);
  std::vector<std::string> not_freed;
  for (auto &block_name: blocks) {
    auto it = block_to_tenant_.find(block_name);
    if(it == block_to_tenant_.end()) {
      not_freed.push_back(block_name);
      continue;
    }
    std::string tenant = it->second; 
    auto jt = allocated_blocks_[tenant].find(block_name);
    if (jt == allocated_blocks_[tenant].end()) {
      throw std::logic_error("Insistency between allocated_blocks and block_to_tenant");
    }
    free_blocks_.insert(*jt);
    allocated_blocks_[tenant].erase(jt);
    block_to_tenant_.erase(it);
  }
  if (!not_freed.empty()) {
    std::string not_freed_string;
    for (const auto &b: not_freed) {
      not_freed_string += (b + "; ");
    }
    throw std::out_of_range("Could not free these blocks because they have not been allocated: " + not_freed_string);
  }
}

void maxmin_block_allocator::add_blocks(const std::vector<std::string> &block_names) {
  std::unique_lock<std::mutex> lock(mtx_);
  free_blocks_.insert(block_names.begin(), block_names.end());
}

void maxmin_block_allocator::remove_blocks(const std::vector<std::string> &block_names) {
  std::unique_lock<std::mutex> lock(mtx_);
  for (const auto &block_name: block_names) {
    auto it = std::find(free_blocks_.begin(), free_blocks_.end(), block_name);
    if (it == free_blocks_.end()) {
      throw std::out_of_range("Trying to remove an allocated block: " + block_name);
    }
    free_blocks_.erase(it);
  }
}

std::size_t maxmin_block_allocator::num_free_blocks() {
  std::unique_lock<std::mutex> lock(mtx_);
  return free_blocks_.size();
}

// Needs to be called with lock
std::size_t maxmin_block_allocator::num_allocated_blocks_unsafe()
{
  std::size_t res = 0;
  for(auto &it: allocated_blocks_)
  {
    res += it.second.size();
  }
  return res;
}

std::size_t maxmin_block_allocator::num_allocated_blocks() {
  std::unique_lock<std::mutex> lock(mtx_);
  return num_allocated_blocks_unsafe();
  
}

std::size_t maxmin_block_allocator::num_total_blocks() {
  std::unique_lock<std::mutex> lock(mtx_);
  return free_blocks_.size() + num_allocated_blocks_unsafe();
}

// Must be called with lock
void maxmin_block_allocator::register_tenant(std::string tenant_id) {
  auto & tenant_blocks = allocated_blocks_[tenant_id];
  tenant_blocks.clear();
}

}
}
