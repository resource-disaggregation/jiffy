#include <random>
#include <algorithm>
#include <iostream>
#include <cassert>
#include <chrono>
#include "maxmin_block_allocator.h"
#include "bheap.h"

namespace jiffy {
namespace directory {

using namespace utils;

maxmin_block_allocator::maxmin_block_allocator(uint32_t num_tenants, uint32_t interval_ms) {
    num_tenants_ = num_tenants;
    total_blocks_ = 0;
    not_allocated_ = 0;
    if(interval_ms > 0) 
    {
      thread_ = std::thread(&maxmin_block_allocator::thread_run, this, interval_ms);
    }
    
    // stats_thread_ = std::thread(&maxmin_block_allocator::stats_thread_run, this, 10);
}

std::vector<std::string> maxmin_block_allocator::allocate(std::size_t count, const std::vector<std::string> &, const std::string &tenant_id) {
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
      if(not_allocated_ == 0) {
        // Need to reclaim block from borrower
        // Pick borrower with max allocation
        // Pick tenant with maximum allocation
        std::size_t max_allocation = 0;
        std::string max_tenant = "$none$";
        for(auto &jt : allocations_) {
          if(jt.second > max_allocation) {
            max_allocation = jt.second;
            max_tenant = jt.first;
          }
        }
        assert(max_tenant != "$none$" && allocations_[max_tenant] > fair_share);
        if(active_blocks_[max_tenant].size() == allocations_[max_tenant]) {
            // Takeaway block from max_tenant
            // TODO: Picking random block for now. Better policy?
            auto idx = static_cast<int64_t>(active_blocks_[max_tenant].size() - 1);
            auto block_it = std::next(active_blocks_[max_tenant].begin(), utils::rand_utils::rand_int64(idx));
            used_bitmap_[*block_it] = false;
            free_blocks_.insert(*block_it);
            active_blocks_[max_tenant].erase(block_it);
        }
        allocations_[max_tenant] -= 1;
      } else {
        not_allocated_ -= 1;
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
  used_bitmap_[*block_it] = true;
  temp_used_bitmap_[*block_it] = true;
  free_blocks_.erase(block_it);

  return append_seq_nos(blocks);
}

void maxmin_block_allocator::free(const std::vector<std::string> &_blocks, const std::string &tenant_id) {
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
    used_bitmap_[*jt] = false;
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
    used_bitmap_[blk] = false;
    temp_used_bitmap_[blk] = false;
  }
  total_blocks_ += block_names.size();
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
    assert(used_bitmap_.erase(block_name) == 1);
    assert(temp_used_bitmap_.erase(block_name) == 1);
    total_blocks_ -= 1;
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
  for(auto &it: active_blocks_)
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
  return total_blocks_;
}

// Must be called with lock
void maxmin_block_allocator::register_tenant(std::string tenant_id) {
  auto & tenant_blocks = active_blocks_[tenant_id];
  tenant_blocks.clear();
  auto fair_share = total_blocks_ / num_tenants_;
  demands_[tenant_id] = fair_share;
  allocations_[tenant_id] = fair_share;
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

void maxmin_block_allocator::update_demand(const std::string &tenant_id, uint32_t demand, uint32_t /*oracle_demand*/) {
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
void maxmin_block_allocator::compute_allocations() {
  std::unique_lock<std::mutex> lock(mtx_);
  if(demands_.size() < num_tenants_) {
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
  std::unordered_map<std::string, uint32_t> demands;
  for(auto &jt : demands_)
  {
    // Adjust demands to deal with situations where tenants advertise low demands, but have still not released the blocks
    demands[jt.first] = std::max(jt.second, (uint32_t)active_blocks_[jt.first].size());
  }
  end_tim = std::chrono::high_resolution_clock::now();
  auto duration_adj_demands = std::chrono::duration_cast<std::chrono::microseconds>( end_tim - start_tim ).count();

  start_tim = std::chrono::high_resolution_clock::now();
  
  // Maxmin algorithm
  maxmin_algorithm_fast(demands);
  
  end_tim = std::chrono::high_resolution_clock::now();
  auto duration_karma_algo = std::chrono::duration_cast<std::chrono::microseconds>( end_tim - start_tim ).count();

  start_tim = std::chrono::high_resolution_clock::now();
  // Adjust blocks to ensure the invariant --- active_blocks.size() <= allocation
  for(auto &jt : active_blocks_) {
    while(jt.second.size() > allocations_[jt.first]) {
      // Take away blocks
      // TODO: Picking random block for now. Better policy?
      auto idx = static_cast<int64_t>(jt.second.size() - 1);
      auto block_it = std::next(jt.second.begin(), utils::rand_utils::rand_int64(idx));
      used_bitmap_[*block_it] = false;
      free_blocks_.insert(*block_it);
      jt.second.erase(block_it);
    }
  }
  end_tim = std::chrono::high_resolution_clock::now();
  auto duration_adj_blocks = std::chrono::duration_cast<std::chrono::microseconds>( end_tim - start_tim ).count();


  start_tim = std::chrono::high_resolution_clock::now();
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
  LOG(log_level::info) << ss.str();
  end_tim = std::chrono::high_resolution_clock::now();
  auto duration_log_state = std::chrono::duration_cast<std::chrono::microseconds>( end_tim - start_tim ).count();

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
  LOG(log_level::info) << "adj_demands took " << duration_adj_demands << "us";
  LOG(log_level::info) << "karma_algo took " << duration_karma_algo << "us";
  LOG(log_level::info) << "adj_blocks took " << duration_adj_blocks << "us";
  LOG(log_level::info) << "log_state took " << duration_log_state << "us";
  LOG(log_level::info) << "reset_bitmap took " << duration_reset_bitmap << "us";
}

void maxmin_block_allocator::thread_run(uint32_t interval_ms) {
  while(true) {
    compute_allocations();
    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
  } 
}


// MUST be called with lock
void maxmin_block_allocator::maxmin_algorithm_fast(std::unordered_map<std::string, uint32_t> &demands) {
  // auto fair_share = total_blocks_ / num_tenants_;

//   total_demand = sum([self.demands[t][e] for t in self.demands])

//     if self.total_blocks >= total_demand:
//     # We can satisfy everyone's demands
//     for t in self.demands:
//         assert len(self.allocations[t]) == e
//         self.allocations[t].append(self.demands[t][e])

    uint32_t total_demand = 0;
    for(auto &jt : demands) {
        total_demand += jt.second;
    }

    if(total_blocks_ >= total_demand) 
    {
        // We can satify everyones demands
        for(auto &jt : demands) {
            allocations_[jt.first] = jt.second;
        }
        not_allocated_ = total_blocks_ - total_demand;
    } else {
        // Waterfilling algorithm
        auto active_tenants = BroadcastHeap();
        for(auto &jt : demands) {
            active_tenants.push(jt.first, jt.second);
            allocations_[jt.first] = 0;
        }

        auto sup = total_blocks_;
        while(sup > 0) {
            if(sup < active_tenants.size()) {
                for(std::size_t i = 0; i < sup; i++) 
                {
                    bheap_item item = active_tenants.pop();
                    auto x = item.second - 1;
                    sup -= 1;
                    allocations_[item.first] = demands[item.first] - x;
                }
            } else {
                auto alpha = std::min(active_tenants.min_val(), (int32_t)(sup/active_tenants.size()));
                assert(alpha > 0);
                active_tenants.add_to_all(-1*alpha);
                sup -= active_tenants.size() * alpha;
            }

            // Get rid of tenants with x = 0
            while(active_tenants.size() > 0 && active_tenants.min_val() == 0) {
                bheap_item item = active_tenants.pop();
                allocations_[item.first] = demands[item.first];
            }
        }

        while(active_tenants.size() > 0) {
            bheap_item item = active_tenants.pop();
            allocations_[item.first] = demands[item.first] - item.second;
        }

        not_allocated_ = 0;

    }

}

}
}
