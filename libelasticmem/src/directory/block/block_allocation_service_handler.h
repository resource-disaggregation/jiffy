#ifndef ELASTICMEM_BLOCK_ALLOCATION_SERVICE_HANDLER_H
#define ELASTICMEM_BLOCK_ALLOCATION_SERVICE_HANDLER_H

#include "block_allocation_service.h"
#include "block_allocator.h"

namespace elasticmem {
namespace directory {

class block_allocation_service_handler: public block_allocation_serviceIf {
 public:
  block_allocation_service_handler(std::shared_ptr<block_allocator> alloc);
  void add_blocks(const std::vector<std::string> &block_names) override;
  void remove_blocks(const std::vector<std::string> &block_names) override;

 private:
  std::shared_ptr<block_allocator> alloc_;
};

}
}

#endif //ELASTICMEM_BLOCK_ALLOCATION_SERVICE_HANDLER_H
