#ifndef MMUX_BLOCK_ALLOCATION_SERVICE_FACTORY_H
#define MMUX_BLOCK_ALLOCATION_SERVICE_FACTORY_H

#include "block_allocation_service.h"
#include "block_allocator.h"

namespace mmux {
namespace directory {
/* Block allocation service factory class */
class block_allocation_service_factory : public block_allocation_serviceIfFactory {
 public:

  /**
   * @brief Constructor
   * @param alloc Block allocator
   */

  explicit block_allocation_service_factory(std::shared_ptr<block_allocator> alloc);

 private:

  /**
   * @brief Get block allocation service handler
   * @param conn_info Connection information
   * @return Block allocation handler
   */

  block_allocation_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;

  /**
   * @brief Release handler
   * @param handler Block allocation handler
   */

  void releaseHandler(block_allocation_serviceIf *anIf) override;

 private:
  /* Block allocator */
  std::shared_ptr<block_allocator> alloc_;
};

}
}

#endif //MMUX_BLOCK_ALLOCATION_SERVICE_FACTORY_H
