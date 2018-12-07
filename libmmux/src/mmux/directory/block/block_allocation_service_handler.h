#ifndef MMUX_BLOCK_ALLOCATION_SERVICE_HANDLER_H
#define MMUX_BLOCK_ALLOCATION_SERVICE_HANDLER_H

#include "block_allocation_service.h"
#include "block_allocator.h"

namespace mmux {
namespace directory {
/* Block allocation service handler class */
class block_allocation_service_handler : public block_allocation_serviceIf {
 public:

  /**
   * @brief Constructor
   * @param alloc Block allocator
   */

  explicit block_allocation_service_handler(std::shared_ptr<block_allocator> alloc);

  /**
   * @brief Add blocks
   * @param block_names Block names
   */

  void add_blocks(const std::vector<std::string> &block_names) override;

  /**
   * @brief Remove blocks
   * @param block_names Block names
   */

  void remove_blocks(const std::vector<std::string> &block_names) override;

 private:

  /**
   * @brief Make exception
   * @param e Exception
   * @return Exception
   */

  block_allocation_service_exception make_exception(const std::out_of_range &e);
  /* Block allocator */
  std::shared_ptr<block_allocator> alloc_;
};

}
}

#endif //MMUX_BLOCK_ALLOCATION_SERVICE_HANDLER_H
