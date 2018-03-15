#include "block_allocation_service_handler.h"

namespace elasticmem {
namespace directory {

block_allocation_service_handler::block_allocation_service_handler(std::shared_ptr<block_allocator> alloc)
    : alloc_(std::move(alloc)) {}

void block_allocation_service_handler::add_blocks(const std::vector<std::string> &block_names) {
  alloc_->add_blocks(block_names);
}

void block_allocation_service_handler::remove_blocks(const std::vector<std::string> &block_names) {
  alloc_->remove_blocks(block_names);
}

}
}