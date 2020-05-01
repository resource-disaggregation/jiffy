#include "allocator_service.h"
#include "jiffy/storage/manager/detail/block_id_parser.h"
#include "jiffy/maestro/free_block_allocator.h"

namespace jiffy {
  namespace maestro {

    allocator_service::allocator_service(std::shared_ptr<free_block_allocator> free_pool,
                                         std::shared_ptr<usage_tracker> usage_tracker)
        : free_block_allocator_(std::move(free_pool)), usage_tracker_(std::move(usage_tracker)) {}

    std::vector<std::string> allocator_service::allocate(const std::string& tenant_ID, int64_t num_blocks) {

      std::vector<std::string> exclude_list;
      auto allocated_blocks = free_block_allocator_->allocate(num_blocks, exclude_list);

      if (allocated_blocks.size() < num_blocks) {
        //todo: Perform reclaim
      }

      usage_tracker_->add_blocks(tenant_ID, allocated_blocks.size());

      return allocated_blocks;
    }

    void allocator_service::deallocate(const std::string& tenant_ID, const std::vector<std::string>& blocks) {

      free_block_allocator_->remove_blocks(blocks);
      usage_tracker_->remove_blocks(tenant_ID, blocks.size());
    }
  }
}