#include <random>
#include <algorithm>
#include <iostream>
#include <cassert>
#include <chrono>
#include "karma_block_allocator.h"

namespace jiffy {
namespace directory {

using namespace utils;

karma_block_allocator::karma_block_allocator(uint32_t num_tenants, uint64_t init_credits, uint32_t interval_ms) {
    num_tenants_ = num_tenants;
    init_credits_ = init_credits;
    total_blocks_ = 0;
    thread_ = std::thread(&karma_block_allocator::thread_run, this, interval_ms);
    stats_thread_ = std::thread(&karma_block_allocator::stats_thread_run, this, 10);
}

std::vector<std::string> karma_block_allocator::allocate(std::size_t count, const std::vector<std::string> &, const std::string &tenant_id) {
  LOG(log_level::info) << "Allocation request for tenant_id: " << tenant_id;
  if(count > 1) {
      throw std::logic_error("Multi-block allocation support not implemented");
  }
  std::unique_lock<std::mutex> lock(mtx_);

  auto my = active_blocks_.find(tenant_id);
  if(my == active_blocks_.end())
  {
    // First time seeing this tenant, register
    register_tenant(tenant_id);
    my = active_blocks_.find(tenant_id);
    assert(my != active_blocks_.end());
  }

  auto fair_share = total_blocks_ / num_tenants_;

  // Tenant is asking for more than what the algorithm allocated
  if(my->second.size() == allocations_[tenant_id]) {
      if(allocations_[tenant_id] >= fair_share) {
          throw std::out_of_range("Could not find free blocks");
      }

      // allocation < fair_share, so we increase the allocation
      assert(rate_[tenant_id] >= 0);
      if(static_cast<uint32_t>(rate_[tenant_id]) == fair_share - allocations_[tenant_id]) {
            // Find a substitute supplier if possible
            // Pick one with minimum no of credits if there are multiple
            LOG(log_level::info) << "Testing uint64 max: " << std::numeric_limits<uint64_t>::max();
            uint64_t poorest_credits = std::numeric_limits<uint64_t>::max();
            std::string poorest_supplier = "$none$";
            for(auto &jt : credits_) {
                assert(rate_[jt.first] >= 0);
                if(allocations_[jt.first] < fair_share && (static_cast<uint32_t>(rate_[jt.first]) < fair_share - allocations_[jt.first])) {
                    if(jt.second < poorest_credits) {
                        poorest_credits = jt.second;
                        poorest_supplier = jt.first;
                    }
                }
            }
            if(poorest_supplier != "$none$") {
                // Adjust credits
                credits_[tenant_id] -= 1;
                rate_[tenant_id] -= 1;
                credits_[poorest_supplier] += 1;
                rate_[poorest_supplier] += 1;

            } else {
                // Need to reclaim block from borrower
                // Pick min-credits borrower
                uint64_t min_credits = std::numeric_limits<uint64_t>::max();
                std::string min_borrower = "$none$";
                for(auto &jt : credits_) {
                    if(allocations_[jt.first] > fair_share) {
                        if(jt.second < min_credits) {
                            min_credits = jt.second;
                            min_borrower = jt.first;
                        }
                    }
                }
                assert(min_borrower != "$none$");
                if(active_blocks_[min_borrower].size() == allocations_[min_borrower]) {
                    // Takeaway block from max_tenant
                    // TODO: Picking random block for now. Better policy?
                    auto idx = static_cast<int64_t>(active_blocks_[min_borrower].size() - 1);
                    auto block_it = std::next(active_blocks_[min_borrower].begin(), utils::rand_utils::rand_int64(idx));
                    free_blocks_.insert(*block_it);
                    active_blocks_[min_borrower].erase(block_it);
                }
                allocations_[min_borrower] -= 1;
                // Adjust credits (give borrower back its credits)
                credits_[min_borrower] += 1;
                rate_[min_borrower] += 1;
                credits_[tenant_id] -= 1;
                rate_[tenant_id] -= 1;
            }
      }   
      allocations_[tenant_id] += 1;
  }

  assert(free_blocks_.size() > 0);

  // Pick random block from free pool
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

void karma_block_allocator::free(const std::vector<std::string> &_blocks, const std::string &tenant_id) {
  LOG(log_level::info) << "Free request for tenant_id: " << tenant_id;
  std::unique_lock<std::mutex> lock(mtx_);
  std::vector<std::string> blocks = strip_seq_nos(_blocks);
  std::vector<std::string> not_freed;
  auto my = active_blocks_.find(tenant_id);
  if(my == active_blocks_.end())
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

void karma_block_allocator::add_blocks(const std::vector<std::string> &block_names) {
  std::unique_lock<std::mutex> lock(mtx_);
  free_blocks_.insert(block_names.begin(), block_names.end());
  for(auto blk : block_names) {
    block_seq_no_[blk] = 0;
    last_tenant_[blk] = "$none$";
  }
  total_blocks_ += block_names.size();
}

void karma_block_allocator::remove_blocks(const std::vector<std::string> &block_names) {
  std::unique_lock<std::mutex> lock(mtx_);
  for (const auto &block_name: block_names) {
    auto it = std::find(free_blocks_.begin(), free_blocks_.end(), block_name);
    if (it == free_blocks_.end()) {
      throw std::out_of_range("Trying to remove an allocated block: " + block_name);
    }
    free_blocks_.erase(it);
    assert(block_seq_no_.erase(block_name) == 1);
    assert(last_tenant_.erase(block_name) == 1);
    total_blocks_ -= 1;
  }
}

std::size_t karma_block_allocator::num_free_blocks() {
  std::unique_lock<std::mutex> lock(mtx_);
  return free_blocks_.size();
}

// Needs to be called with lock
std::size_t karma_block_allocator::num_allocated_blocks_unsafe()
{
  std::size_t res = 0;
  for(auto &it: active_blocks_)
  {
    res += it.second.size();
  }
  return res;
}

std::size_t karma_block_allocator::num_allocated_blocks() {
  std::unique_lock<std::mutex> lock(mtx_);
  return num_allocated_blocks_unsafe();
  
}

std::size_t karma_block_allocator::num_total_blocks() {
  std::unique_lock<std::mutex> lock(mtx_);
  return total_blocks_;
}

// Must be called with lock
void karma_block_allocator::register_tenant(std::string tenant_id) {
  auto & tenant_blocks = active_blocks_[tenant_id];
  tenant_blocks.clear();
  auto fair_share = total_blocks_ / num_tenants_;
  demands_[tenant_id] = fair_share;
  credits_[tenant_id] = init_credits_;
  rate_[tenant_id] = 0;
  allocations_[tenant_id] = fair_share;
}

// Must be called with lock
std::vector<std::string> karma_block_allocator::append_seq_nos(const std::vector<std::string> &blocks) {
  std::vector<std::string> res;
  for(auto block : blocks) {
    res.push_back(block + ":" + std::to_string(block_seq_no_[block]));
  }
  return res;
}

// Must be called with lock
std::vector<std::string> karma_block_allocator::strip_seq_nos(const std::vector<std::string> &blocks) {
  std::vector<std::string> res;
  for(auto block : blocks) {
    auto pos = block.find_last_of(':');
    assert(pos != std::string::npos);
    res.push_back(block.substr(0, pos));
  }
  return res;
}

void karma_block_allocator::update_demand(const std::string &tenant_id, uint32_t demand) {
    LOG(log_level::info) << "Demand advertisement: " << tenant_id << " " << demand;
    std::unique_lock<std::mutex> lock(mtx_);
    auto my = demands_.find(tenant_id);
    if(my == demands_.end())
    {
      // First time seeing this tenant, register
      register_tenant(tenant_id);
      my = demands_.find(tenant_id);
      assert(my != demands_.end());
    }
    my->second = demand;
}

// Compute allocations and adjust blocks
// Also updates credits, rates
void karma_block_allocator::compute_allocations() {
  std::unique_lock<std::mutex> lock(mtx_);
  if(demands_.size() == 0) {
    return;
  }
  auto fair_share = total_blocks_ / num_tenants_;
  std::unordered_map<std::string, uint32_t> demands;
  for(auto &jt : demands_)
  {
    // Adjust demands to deal with situations where tenants advertise low demands, but have still not released the blocks
    demands[jt.first] = std::max(jt.second, (uint32_t)active_blocks_[jt.first].size());
  }

  // Reset rates
  for(auto &jt : rate_) {
    jt.second = 0;
  }

  for(auto &jt : demands) {
    allocations_[jt.first] = std::min(jt.second, (uint32_t) fair_share);
  }

  // Iteratively match donors to borrowers
  // TODO: optimize this implementation
  while(true) {
    // Pick richest borrower
    std::string borrower = "$none$";
    uint64_t richest_credits = 0;
    for(auto &jt : credits_) {
      if(demands[jt.first] > fair_share && allocations_[jt.first] < demands[jt.first] && jt.second > 0 && jt.second > richest_credits) {
        borrower = jt.first;
        richest_credits = jt.second;
      }
    }
    if(borrower == "$none$") {
      break;
    }

    // Pick poorest donor
    std::string donor = "$none$";
    uint64_t poorest_credits = std::numeric_limits<uint64_t>::max();
    for(auto &jt : credits_) 
    {
      if(demands[jt.first] < fair_share && (uint32_t)rate_[jt.first] < fair_share - demands[jt.first] && jt.second < poorest_credits) {
        donor = jt.first;
        poorest_credits = jt.second;
      }
    }
    if(donor  == "$none$") {
      break;
    }

    // Block transaction
    allocations_[borrower] += 1;
    // LOG(log_level::info) << "borrower, alloc=" << allocations_[borrower] << " " << borrower;
    credits_[borrower] -= 1;
    // LOG(log_level::info) << "borrower, c=" << credits_[borrower] << " " << borrower;
    rate_[borrower] -= 1;
    credits_[donor] += 1;
    // LOG(log_level::info) << "donor, c=" << credits_[donor] << " " << donor;
    rate_[donor] += 1;
  }

  // Adjust blocks to ensure the invariant --- active_blocks.size() <= allocation
  for(auto &jt : active_blocks_) {
    while(jt.second.size() > allocations_[jt.first]) {
      // Take away blocks
      // TODO: Picking random block for now. Better policy?
      auto idx = static_cast<int64_t>(jt.second.size() - 1);
      auto block_it = std::next(jt.second.begin(), utils::rand_utils::rand_int64(idx));
      free_blocks_.insert(*block_it);
      jt.second.erase(block_it);
    }
  }

  // Log state
  std::stringstream ss;
  ss << "Demands: ";
  for(auto &jt : demands) {
    ss << jt.first << " " << jt.second << " ";
  }
  ss << "Allocs: ";
  for(auto &jt : allocations_) 
  {
    ss << jt.first << " " << jt.second << " ";
  }
  ss << "Credits: ";
  for(auto &jt : credits_)
  {
    ss << jt.first << " " << jt.second << " ";
  }
  LOG(log_level::info) << ss.str(); 

}

void karma_block_allocator::log_stats() {
  std::unique_lock<std::mutex> lock(mtx_);
  if(demands_.size() == 0) {
    return;
  }
  LOG(log_level::info) << "Utilization: " << ((double)(total_blocks_ - free_blocks_.size()))/((double)total_blocks_);
}

void karma_block_allocator::thread_run(uint32_t interval_ms) {
  while(true) {
    compute_allocations();
    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
  } 
}

void karma_block_allocator::stats_thread_run(uint32_t interval_ms) {
  while(true) {
    log_stats();
    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
  }
}

}
}
