#ifndef MMUX_BLOCK_ALLOCATION_SERVICE_HANDLER_H
#define MMUX_BLOCK_ALLOCATION_SERVICE_HANDLER_H

#include "block_allocation_service.h"
#include "block_allocator.h"

namespace mmux {
namespace directory {

class block_allocation_service_handler: public block_allocation_serviceIf {
 public:
  explicit block_allocation_service_handler(std::shared_ptr<block_allocator> alloc);
  void add_blocks(const std::vector<std::string> &block_names) override;
  void remove_blocks(const std::vector<std::string> &block_names) override;

 private:
  block_allocation_service_exception make_exception(const std::out_of_range& e);
  std::shared_ptr<block_allocator> alloc_;
};

}
}

#endif //MMUX_BLOCK_ALLOCATION_SERVICE_HANDLER_H
