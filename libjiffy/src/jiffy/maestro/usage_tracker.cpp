#include "usage_tracker.h"
#include "maestro_ops_exception.h"
#include <jiffy/utils/logger.h>
#include "math.h"

namespace jiffy {
  namespace maestro {

    using namespace ::jiffy::utils;

    usage_tracker::usage_tracker(std::shared_ptr<free_block_allocator> free_pool,
                                 std::unordered_map<std::string, float> &distribution_by_tenant)
        : free_pool(std::move(free_pool)), distribution_by_tenant_(distribution_by_tenant) {}

    void usage_tracker::add_blocks(const std::string &tenant_id, std::int32_t num_blocks) {
      std::unique_lock<std::mutex> lock(mtx_);
      num_assigned_blocks_by_tenant_[tenant_id] += num_blocks;
      LOG(log_level::info) << " Tenant: " << tenant_id
                                << " total blocks: "<< num_assigned_blocks_by_tenant_[tenant_id]
                                << " added: " << num_blocks;
    }

    void usage_tracker::remove_blocks(const std::string &tenant_id, std::int32_t num_blocks) {
      std::unique_lock<std::mutex> lock(mtx_);
      if (num_blocks > num_assigned_blocks_by_tenant_[tenant_id]) {
        throw maestro_ops_exception("Illegal State - tenant usage negative");
      }
      num_assigned_blocks_by_tenant_[tenant_id] -= num_blocks;
      LOG(log_level::info) << " Tenant: " << tenant_id
                                << " total blocks: "<< num_assigned_blocks_by_tenant_[tenant_id]
                                << " removed: " << num_blocks;
    }

    int32_t usage_tracker::num_under_provisioned_blocks(const std::string &tenant_ID) {
      std::unique_lock<std::mutex> lock(mtx_);

      size_t current_share = 0;
      float fair_share_percent = 0;

      std::unordered_map<std::string, float>::const_iterator fair_share_it = distribution_by_tenant_.find(tenant_ID);
      if(fair_share_it == distribution_by_tenant_.end()) {
        throw maestro_ops_exception("Illegal State - Reclaim called for unknown tenant");
      }
      fair_share_percent = fair_share_it->second;

      std::unordered_map<std::string, std::size_t>::const_iterator current_share_it = num_assigned_blocks_by_tenant_.find(tenant_ID);
      if(current_share_it != num_assigned_blocks_by_tenant_.end()) {
        current_share = current_share_it->second;
      }

      auto total_num_blocks = free_pool->num_total_blocks();
      auto fair_share = floor(total_num_blocks * fair_share_percent);
      return fair_share - current_share;
    }

    //todo: optimize peformance
    tenant usage_tracker::get_max_borrowing_tenant() {
      std::unique_lock<std::mutex> lock(mtx_);

      auto total_num_blocks = free_pool->num_total_blocks();
      tenant max_borrower;

      for(auto & it : num_assigned_blocks_by_tenant_) {

        auto tenentId = it.first;
        auto num_blocks = it.second;
        auto fair_share = floor(total_num_blocks * distribution_by_tenant_[tenentId]);
        auto excess_blocks = num_blocks - fair_share;
        if(excess_blocks > max_borrower.num_over_provisioned_blocks) {
          max_borrower.id = tenentId;
          max_borrower.num_block = num_blocks;
          max_borrower.num_over_provisioned_blocks = excess_blocks;
        }
      }
      return max_borrower;
    }


//    void usage_tracker::update_fair_distribution(std::unordered_map<std::string, std::size_t> &distribution_by_tenant) {
//
//      std::unique_lock<std::mutex> lock(mtx_);
//      float distribution_sum = 0;
//      for (auto &mem_share: distribution_by_tenant) {
//        distribution_sum += mem_share.second;
//      }
//      if (distribution_sum == 0) {
//        throw maestro_ops_exception("Bad Request - distribution invalid");
//      }
//
//      auto total_num_blocks = free_pool->num_total_blocks();
//      for (auto &mem_share: distribution_by_tenant) {
//        float capacityPercentage = distribution_by_tenant[mem_share.first] / distribution_sum;
//        distribution_by_tenant[mem_share.first] = floor(total_num_blocks * capacityPercentage);
//      }
//
//      distribution_by_tenant_ = distribution_by_tenant;
//    }

  }
}