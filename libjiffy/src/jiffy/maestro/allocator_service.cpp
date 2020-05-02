#include "allocator_service.h"
#include "jiffy/storage/manager/detail/block_id_parser.h"
#include "jiffy/maestro/free_block_allocator.h"

namespace jiffy {
  namespace maestro {

    allocator_service::allocator_service(std::shared_ptr<free_block_allocator> free_pool,
                                         std::shared_ptr<usage_tracker> usage_tracker,
                                         std::shared_ptr<reclaim_service> reclaim_service)
        : free_block_allocator_(std::move(free_pool)),
          usage_tracker_(std::move(usage_tracker)),
          reclaim_service_(std::move(reclaim_service)) {}

    std::vector<std::string> allocator_service::allocate(const std::string& tenant_ID, int64_t num_blocks) {

      std::vector<std::string> exclude_list;
      auto allocated_blocks = free_block_allocator_->allocate(num_blocks, exclude_list);
      usage_tracker_->add_blocks(tenant_ID, allocated_blocks.size());

      if (allocated_blocks.size() < num_blocks) {
        auto num_reclaimable_blocks = usage_tracker_->num_under_provisioned_blocks(tenant_ID);
        if(num_reclaimable_blocks > 0) {
          auto reclaimed_blocks = reclaim_service_->reclaim(tenant_ID, num_reclaimable_blocks);
          usage_tracker_->add_blocks(tenant_ID, reclaimed_blocks.size());
          allocated_blocks.insert(allocated_blocks.end(), reclaimed_blocks.begin(), reclaimed_blocks.end());
        }
      }

      return allocated_blocks;
    }

    void allocator_service::deallocate(const std::string& tenant_ID, const std::vector<std::string>& blocks) {

      free_block_allocator_->remove_blocks(blocks);
      usage_tracker_->remove_blocks(tenant_ID, blocks.size());
    }
  }
}