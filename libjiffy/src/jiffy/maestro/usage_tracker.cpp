#include "usage_tracker.h"
#include "maestro_ops_exception.h"
#include <jiffy/utils/logger.h>

namespace jiffy {
  namespace maestro {

    using namespace ::jiffy::utils;

    usage_tracker::usage_tracker(std::shared_ptr<free_block_allocator> free_pool)
        : free_block_allocator_(std::move(free_pool)) {}

    void usage_tracker::add_blocks(const std::string &tenant_id, std::size_t num_blocks) {
      std::unique_lock<std::mutex> lock(mtx_);
      num_assigned_blocks_by_tenant_[tenant_id] += num_blocks;
      LOG(log_level::info) << " Tenant: " << tenant_id
                                << " total blocks: "<< num_assigned_blocks_by_tenant_[tenant_id]
                                << " added: " << num_blocks;
    }

    void usage_tracker::remove_blocks(const std::string &tenant_id, std::size_t num_blocks) {
      std::unique_lock<std::mutex> lock(mtx_);
      if (num_blocks > num_assigned_blocks_by_tenant_[tenant_id]) {
        throw maestro_ops_exception("Illegal State - tenant usage negative");
      }
      num_assigned_blocks_by_tenant_[tenant_id] -= num_blocks;
      LOG(log_level::info) << " Tenant: " << tenant_id
                                << " total blocks: "<< num_assigned_blocks_by_tenant_[tenant_id]
                                << " removed: " << num_blocks;
    }


//    void usage_tracker::update_fair_distribution(std::unordered_map<std::string, std::size_t> &distribution_by_tenant) {
//
//      std::unique_lock<std::mutex> lock(mtx_);
//      std::float_t distribution_sum = 0;
//      for (auto &mem_share: distribution_by_tenant) {
//        distribution_sum += mem_share.second;
//      }
//      if (distribution_sum == 0) {
//        throw maestro_ops_exception("Bad Request - distribution invalid");
//      }
//
//      auto total_num_blocks = free_block_allocator_->num_total_blocks();
//      for (auto &mem_share: distribution_by_tenant) {
//        float_t capacityPercentage = distribution_by_tenant[mem_share.first] / distribution_sum;
//        distribution_by_tenant[mem_share.first] = floor(total_num_blocks * capacityPercentage);
//      }
//
//      distribution_by_tenant_ = distribution_by_tenant;
//    }

  }
}