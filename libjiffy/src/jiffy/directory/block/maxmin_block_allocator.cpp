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
  // Increment sequence number if ownership transfer
  if(last_tenant_[*block_it] != tenant_id) {
    block_seq_no_[*block_it] += 1;
  }
  my->second.insert(*block_it);
  blocks.push_back(*block_it);
  last_tenant_[*block_it] = tenant_id;
  free_blocks_.erase(block_it);

  return append_seq_nos(blocks);
}

void maxmin_block_allocator::free(const std::vector<std::string> &_blocks, const std::string &tenant_id) {
  LOG(log_level::info) << "Free request for tenant_id: " << tenant_id;
  std::unique_lock<std::mutex> lock(mtx_);
  std::vector<std::string> blocks = strip_seq_nos(_blocks);
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

  // TODO: Handle spurious deallocation that may be caused due to reclaims 
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
  for(auto blk : block_names) {
    block_seq_no_[blk] = 0;
    last_tenant_[blk] = "$none$";
  }
}

void maxmin_block_allocator::remove_blocks(const std::vector<std::string> &block_names) {
  std::unique_lock<std::mutex> lock(mtx_);
  for (const auto &block_name: block_names) {
    auto it = std::find(free_blocks_.begin(), free_blocks_.end(), block_name);
    if (it == free_blocks_.end()) {
      throw std::out_of_range("Trying to remove an allocated block: " + block_name);
    }
    free_blocks_.erase(it);
    assert(block_seq_no_.erase(block_name) == 1);
    assert(last_tenant_.erase(block_name) == 1);
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

// Must be called with lock
std::vector<std::string> maxmin_block_allocator::append_seq_nos(const std::vector<std::string> &blocks) {
  std::vector<std::string> res;
  for(auto block : blocks) {
    res.push_back(block + ":" + std::to_string(block_seq_no_[block]));
  }
  return res;
}

// Must be called with lock
std::vector<std::string> maxmin_block_allocator::strip_seq_nos(const std::vector<std::string> &blocks) {
  std::vector<std::string> res;
  for(auto block : blocks) {
    auto pos = block.find_last_of(':');
    assert(pos != std::string::npos);
    res.push_back(block.substr(0, pos));
  }
  return res;
}

}
}
