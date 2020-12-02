#include <random>
#include <algorithm>
#include <iostream>
#include <cassert>
#include "static_block_allocator.h"

namespace jiffy {
namespace directory {

using namespace utils;

static_block_allocator::static_block_allocator(uint32_t num_tenants, uint32_t interval_ms) {
    num_tenants_ = num_tenants;
    total_blocks_ = 0;
    if(interval_ms > 0) 
    {
      thread_ = std::thread(&static_block_allocator::thread_run, this, interval_ms);
    }
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
  used_bitmap_[*block_it] = true;
  temp_used_bitmap_[*block_it] = true;
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
    used_bitmap_[*jt] = false;
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
  for(auto blk : block_names) {
    used_bitmap_[blk] = false;
    temp_used_bitmap_[blk] = false;
  }
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
    assert(used_bitmap_.erase(block_name) == 1);
    assert(temp_used_bitmap_.erase(block_name) == 1);
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

void static_block_allocator::thread_run(uint32_t interval_ms) {
  while(true) {
    compute_allocations();
    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
  } 
}

void static_block_allocator::compute_allocations() {
  std::unique_lock<std::mutex> lock(mtx_);
  if(allocated_blocks_.size() < num_tenants_) {
    return;
  }

  auto t1 = std::chrono::high_resolution_clock::now();

  auto start_tim = std::chrono::high_resolution_clock::now();
  // Log utilization for previous epoch
  std::size_t num_used_blocks = 0;
  for(auto &jt : temp_used_bitmap_) {
    if(jt.second) {
      num_used_blocks += 1;
    }
  }

  auto end_tim = std::chrono::high_resolution_clock::now();

  LOG(log_level::info) << "Epoch used blocks: " << num_used_blocks;
  auto duration_log_util = std::chrono::duration_cast<std::chrono::microseconds>( end_tim - start_tim ).count();

  start_tim = std::chrono::high_resolution_clock::now();
  // Reset temp bitmap
  for(auto &jt : temp_used_bitmap_) {
    jt.second = used_bitmap_[jt.first];
  }
  end_tim = std::chrono::high_resolution_clock::now();
  auto duration_reset_bitmap = std::chrono::duration_cast<std::chrono::microseconds>( end_tim - start_tim ).count();

  auto t2 = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>( t2 - t1 ).count();
  LOG(log_level::info) << "compute_allocations took " << duration << " us";
  LOG(log_level::info) << "log_util took " << duration_log_util << " us";
  LOG(log_level::info) << "reset_bitmap took " << duration_reset_bitmap << "us";
}

}
}







