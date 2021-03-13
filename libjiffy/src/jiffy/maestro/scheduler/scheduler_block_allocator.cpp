#include <jiffy/maestro/maestro_ops_exception.h>

#include <utility>
#include "scheduler_block_allocator.h"
#include <jiffy/utils/logger.h>


namespace jiffy {
  namespace maestro {

    using namespace utils;

    scheduler_block_allocator::scheduler_block_allocator(std::string tenantID,
                                                         std::shared_ptr<maestro_allocator_client> client)
                                                         : tenantID_(std::move(tenantID)), client_(std::move(client)) {}

    std::vector<std::string>
    scheduler_block_allocator::allocate(std::size_t count, const std::vector<std::string> &exclude_list) {

      //todo: start timer
      LOG(info) << "Making allocation request for " << count << " blocks.";
      return  client_->allocate(tenantID_, count);
    }

    void scheduler_block_allocator::free(const std::vector<std::string> &block_names) {

      LOG(info) << "Making deallocation request for " << block_names.size() << " blocks.";
      client_->deallocate(tenantID_, block_names);
    }

    void scheduler_block_allocator::add_blocks(const std::vector<std::string> &block_names) {

      throw maestro_ops_exception("Block addition in scheduler is not supported");
    }

    void scheduler_block_allocator::remove_blocks(const std::vector<std::string> &block_names) {

      throw maestro_ops_exception("Block removal in scheduler is not supported");
    }

    std::size_t scheduler_block_allocator::num_free_blocks() {

      throw maestro_ops_exception("num_free_blocks in scheduler is not supported");
    }

    std::size_t scheduler_block_allocator::num_allocated_blocks() {

      throw maestro_ops_exception("num_allocated_blocks in scheduler is not supported");
    }

    std::size_t scheduler_block_allocator::num_total_blocks() {

      throw maestro_ops_exception("num_total_blocks in scheduler is not supported");
    }
  }
}