#ifndef JIFFY_BLOCK_ALLOCATION_SERVICE_HANDLER_H
#define JIFFY_BLOCK_ALLOCATION_SERVICE_HANDLER_H

#include "block_registration_service.h"
#include "block_allocator.h"

namespace jiffy {
namespace directory {
/* Block allocation service handler class */
class block_registration_service_handler : public block_registration_serviceIf {
 public:

  /**
   * @brief Constructor
   * @param alloc Block allocator
   */

  explicit block_registration_service_handler(std::shared_ptr<block_allocator> alloc);

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

  block_registration_service_exception make_exception(const std::out_of_range &e);
  /* Block allocator */
  std::shared_ptr<block_allocator> alloc_;
};

}
}

#endif //JIFFY_BLOCK_ALLOCATION_SERVICE_HANDLER_H
