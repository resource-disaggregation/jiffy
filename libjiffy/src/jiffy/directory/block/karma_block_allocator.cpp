#include <random>
#include <algorithm>
#include <iostream>
#include <cassert>
#include <chrono>
#include "karma_block_allocator.h"
#include "bheap.h"

namespace jiffy {
namespace directory {

using namespace utils;

karma_block_allocator::karma_block_allocator(uint32_t num_tenants, uint64_t init_credits, uint32_t interval_ms, uint32_t public_blocks) {
    num_tenants_ = num_tenants;
    init_credits_ = init_credits;
    total_blocks_ = 0;
    public_blocks_ = public_blocks;
    if(interval_ms > 0) 
    {
      thread_ = std::thread(&karma_block_allocator::thread_run, this, interval_ms);
    }

    rate_["$public$"] = 0;
    credits_["$public$"] = 0;
    
    // stats_thread_ = std::thread(&karma_block_allocator::stats_thread_run, this, 10);
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

  auto fair_share = (total_blocks_ - public_blocks_) / num_tenants_;

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
            // LOG(log_level::info) << "Testing uint64 max: " << std::numeric_limits<uint64_t>::max();
            uint64_t poorest_credits = std::numeric_limits<uint64_t>::max();
            std::string poorest_supplier = "$none$";
            for(auto &jt : credits_) {
                if(jt.first == "$public$") {
                  continue;
                }
                if(allocations_[jt.first] < fair_share && (static_cast<uint32_t>(rate_[jt.first]) < fair_share - allocations_[jt.first])) {
                    assert(rate_[jt.first] >= 0);
                    if(jt.second < poorest_credits) {
                        poorest_credits = jt.second;
                        poorest_supplier = jt.first;
                    }
                }
            }
            if(poorest_supplier == "$none$" && rate_["$public$"] < (int32_t) public_blocks_) {
              poorest_supplier = "$public$";
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
                    if(jt.first == "$public$") {
                      continue;
                    }
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
                    used_bitmap_[*block_it] = false;
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
  // auto block_it = std::next(free_blocks_.begin(), utils::rand_utils::rand_int64(idx));
  auto block_it = free_blocks_.begin();
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

void karma_block_allocator::add_blocks(const std::vector<std::string> &block_names) {
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
    assert(used_bitmap_.erase(block_name) == 1);
    assert(temp_used_bitmap_.erase(block_name) == 1);
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
  auto fair_share = (total_blocks_ - public_blocks_) / num_tenants_;
  demands_[tenant_id] = fair_share;
  oracle_demands_[tenant_id] = fair_share;
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

void karma_block_allocator::update_demand(const std::string &tenant_id, uint32_t demand, uint32_t oracle_demand) {
    LOG(log_level::info) << "Demand advertisement: " << tenant_id << " " << demand << " " << oracle_demand;
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
    oracle_demands_[tenant_id] = oracle_demand;
}

// Compute allocations and adjust blocks
// Also updates credits, rates
void karma_block_allocator::compute_allocations() {
  std::unique_lock<std::mutex> lock(mtx_);
  if(demands_.size() < num_tenants_) {
    return;
  }

  auto t1 = std::chrono::high_resolution_clock::now();

  auto start_tim = std::chrono::high_resolution_clock::now();
  // Log utilization for previous epoch
  std::size_t num_used_blocks = 0;
  std::unordered_map<std::string, std::size_t> tenant_used_blocks;
  for(auto &jt : temp_used_bitmap_) {
    if(jt.second) {
      num_used_blocks += 1;
      tenant_used_blocks[last_tenant_[jt.first]] += 1;
    }
  }

  auto end_tim = std::chrono::high_resolution_clock::now();

  LOG(log_level::info) << "Epoch used blocks: " << num_used_blocks;
  
  std::stringstream ss1;
  ss1 << "Tenant used blocks: ";
  for(auto &jt : demands_) {
    ss1 << jt.first << " " << tenant_used_blocks[jt.first] << " ";
  }

  LOG(log_level::info) << ss1.str();

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
  
  // Karma algorithm
  karma_algorithm_fast(demands);
  
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
  ss << "Credits: ";
  for(auto &jt : credits_)
  {
    ss << jt.first << " " << jt.second << " ";
  }
  ss << "oracle_demands: ";
  for(auto &jt : oracle_demands_) {
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


// MUST be called with lock
void karma_block_allocator::karma_algorithm(std::unordered_map<std::string, uint32_t> &demands) {
  auto fair_share = (total_blocks_ - public_blocks_) / num_tenants_;
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
      if(jt.first == "$public$") {
        continue;
      }
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
      if(jt.first == "$public$") {
        continue;
      }
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
}

// MUST be called with lock
void karma_block_allocator::karma_algorithm_fast(std::unordered_map<std::string, uint32_t> &demands) {
  auto fair_share = (total_blocks_ - public_blocks_) / num_tenants_;
  // Reset rates
  for(auto &jt : rate_) {
    jt.second = 0;
  }

  // Re-distribute public credits
  if(credits_["$public$"] >= num_tenants_) {
    LOG(log_level::info) << "Re-distributing public credits";
    for(auto &jt : credits_) {
      if(jt.first == "$public$") {
        continue;
      }
      jt.second += credits_["$public$"] / num_tenants_;
    }
    credits_["$public$"] = credits_["$public$"] % num_tenants_;
  }

  std::vector<std::string> donors, borrowers;
  uint32_t total_supply = 0;
  uint32_t total_demand = 0;
  for(auto &jt : demands) {
    if(jt.second < fair_share) {
      donors.push_back(jt.first);
      total_supply += fair_share - jt.second;
    }
    else if(jt.second > fair_share) {
      borrowers.push_back(jt.first);
      total_demand += std::min(jt.second - fair_share, credits_[jt.first]);
    }
    // Allocate upto fair share
    allocations_[jt.first] = std::min(jt.second, (uint32_t) fair_share);
  }

  total_supply += public_blocks_;

  // Match supply to demand
  if(total_supply >= total_demand) {
    borrow_from_poorest_fast(demands, donors, borrowers);
  }
  else if(total_supply < total_demand) 
  {
    give_to_richest_fast(demands, donors, borrowers);
  }

  // Update credits based on computed rates
  for(auto &jt : rate_) {
    credits_[jt.first] += jt.second;
  }

}

namespace {
  struct karma_candidate {
    std::string id;
    int64_t c;
    uint32_t x;
  };

  bool poorer(const karma_candidate& a, const karma_candidate& b) 
  {
    return a.c < b.c;
  }

  bool richer(const karma_candidate& a, const karma_candidate& b) 
  {
    return a.c > b.c;
  }
}

// MUST be called with lock
// Note this does NOT update credits_
void karma_block_allocator::borrow_from_poorest_fast(std::unordered_map<std::string, uint32_t> &demands, std::vector<std::string>& donors, std::vector<std::string>& borrowers) {

  auto fair_share = (total_blocks_ - public_blocks_) / num_tenants_;

  // Can satisfy demands of all borrowers
  uint32_t total_demand = 0;
  for(auto &b : borrowers) {
    auto to_borrow = std::min(credits_[b], demands[b] - fair_share);
    allocations_[b] += to_borrow;
    rate_[b] -= to_borrow;
    total_demand += to_borrow;
  }


  // Borrow from poorest donors and update their rates
  std::vector<karma_candidate> donor_list;
  for(auto &d : donors) {
    karma_candidate elem;
    elem.id = d;
    elem.c = credits_[d];
    elem.x = fair_share - demands[d];
    donor_list.push_back(elem);
  }
  // Give normal donors priority over public pool
  if(public_blocks_ > 0)
  {
      donor_list.push_back({"$public$", (int64_t)(num_tenants_*init_credits_ + 777777), public_blocks_});
  }
  donor_list.push_back({"$dummy$", std::numeric_limits<int64_t>::max(), 0});

  // Sort donor_list by credits
  std::sort(donor_list.begin(), donor_list.end(), poorer);

  auto dem = total_demand;
  int64_t cur_c = -1;
  auto next_c = donor_list[0].c;
  // poorest active donor set (heap internally ordered by x)
  auto poorest_donors = BroadcastHeap();
  std::size_t idx = 0;

  while(dem > 0) {
    // LOG(log_level::info) << "Entering while";
    // Update poorest donors
    if(poorest_donors.size() == 0) {
      cur_c = next_c;
      assert(cur_c != std::numeric_limits<int64_t>::max());
    }
    while(donor_list[idx].c == cur_c) {
      poorest_donors.push(donor_list[idx].id, donor_list[idx].x);
      idx += 1;
    }
    next_c = donor_list[idx].c;

    // Perform c,x update
    if(dem < poorest_donors.size()) 
    {
      for(std::size_t i = 0; i < dem; i++) {
        bheap_item item = poorest_donors.pop();
        auto x = item.second - 1;
        dem -= 1;
        auto base_val = (item.first != "$public$")?(fair_share - demands[item.first]):(public_blocks_);
        rate_[item.first] += base_val - x;
      }
    } else {
      auto alpha = (int32_t) std::min({(int64_t)poorest_donors.min_val(), (int64_t)(dem/poorest_donors.size()), (int64_t)(next_c - cur_c)});
      assert(alpha > 0);
      poorest_donors.add_to_all(-1*alpha);
      cur_c += alpha;
      dem -= poorest_donors.size() * alpha;
    }

    // get rid of donors with x = 0
    while(poorest_donors.size() > 0 && poorest_donors.min_val() == 0) {
      bheap_item item = poorest_donors.pop();
      auto base_val = (item.first != "$public$")?(fair_share - demands[item.first]):(public_blocks_);
      rate_[item.first] += base_val;
    }

  }

  while(poorest_donors.size() > 0) {
    bheap_item item = poorest_donors.pop();
    auto base_val = (item.first != "$public$")?(fair_share - demands[item.first]):(public_blocks_);
    rate_[item.first] += base_val - item.second;
  }

}

// MUST be called with lock
// Note this does NOT update credits_
void karma_block_allocator::give_to_richest_fast(std::unordered_map<std::string, uint32_t> &demands, std::vector<std::string>& donors, std::vector<std::string>& borrowers) {

  auto fair_share = (total_blocks_ - public_blocks_) / num_tenants_;

  // Can match all donations
  uint32_t total_supply = 0;
  for(auto &d : donors) {
    auto to_give = fair_share - demands[d];
    rate_[d] += to_give;
    total_supply += to_give;
  }
  if(public_blocks_ > 0) {
    rate_["$public$"] += public_blocks_;
    total_supply += public_blocks_; 
  }

  // # Give to richest borrowers and update their allocations/rate
  std::vector<karma_candidate> borrower_list;
  for(auto &b : borrowers) {
    karma_candidate elem;
    elem.id = b;
    elem.c = credits_[b];
    elem.x = std::min(credits_[b], demands[b] - fair_share);
    borrower_list.push_back(elem);
  }
  borrower_list.push_back({"$dummy$", -1, 0});

  std::sort(borrower_list.begin(), borrower_list.end(), richer);

  auto sup = total_supply;
  int64_t cur_c = std::numeric_limits<int64_t>::max();
  int64_t next_c = borrower_list[0].c;
  // richest active borrower set (heap internally ordered by x)
  auto richest_borrowers = BroadcastHeap();
  std::size_t idx = 0;

  while(sup > 0) {
    // Update richest borrowers
    if(richest_borrowers.size() == 0) {
      cur_c = next_c;
      assert(cur_c != std::numeric_limits<int64_t>::min());
    }
    while(borrower_list[idx].c == cur_c) {
      richest_borrowers.push(borrower_list[idx].id, borrower_list[idx].x);
      idx += 1;
    }
    next_c = borrower_list[idx].c;

    // perform c,x update
    if(sup < richest_borrowers.size()) {
      for(std::size_t i = 0; i < sup; i++) {
        bheap_item item = richest_borrowers.pop();
        auto x = item.second - 1;
        sup -= 1;
        auto delta = std::min(credits_[item.first], demands[item.first] - fair_share) - x;
        allocations_[item.first] += delta;
        rate_[item.first] -= delta;
      }
    } else {
      auto alpha = (int32_t) std::min((int64_t)(richest_borrowers.min_val()), (int64_t)(sup/richest_borrowers.size()));
      assert(alpha > 0);
      if(next_c != -1) {
        alpha = (int32_t) std::min((int64_t)alpha, (int64_t)(cur_c - next_c));
      }
      richest_borrowers.add_to_all(-1 * alpha);
      cur_c -= alpha;
      sup -= richest_borrowers.size() * alpha;
    }

    // Get rid of borrowers with x = 0
    while(richest_borrowers.size() > 0 && richest_borrowers.min_val() == 0) {
      bheap_item item = richest_borrowers.pop();
      auto delta = std::min(credits_[item.first], demands[item.first] - fair_share);
      allocations_[item.first] += delta;
      rate_[item.first] -= delta;
    }
  }

  while(richest_borrowers.size() > 0) {
    bheap_item item = richest_borrowers.pop();
    auto delta = std::min(credits_[item.first], demands[item.first] - fair_share) - item.second;
    allocations_[item.first] += delta;
    rate_[item.first] -= delta;
  }

}

}
}
