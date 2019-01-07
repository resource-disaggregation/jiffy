#ifndef MMUX_BLOCK_REQUEST_HANDLER_FACTORY_H
#define MMUX_BLOCK_REQUEST_HANDLER_FACTORY_H

#include "block_request_service.h"
#include "../chain_module.h"

namespace mmux {
namespace storage {
/* Block request handler factory */
class block_request_handler_factory : public block_request_serviceIfFactory {
 public:
  /**
   * @brief Constructor
   * @param blocks Data blocks
   */

  explicit block_request_handler_factory(std::vector<std::shared_ptr<chain_module>> &blocks);

  /**
   * @brief Fetch block request handler
   * @param connInfo Connection information
   * @return Block request handler
   */

  block_request_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;

  /**
   * @brief Release handler
   * Remove the registered client from the block response client list
   * @param anIf Block request handler
   */

  void releaseHandler(block_request_serviceIf *anIf) override;

 public:
  /* Data blocks */
  std::vector<std::shared_ptr<chain_module>> &blocks_;
  /* Client identifier generator, starts at 1 */
  std::atomic<int64_t> client_id_gen_;
};

}
}

#endif //MMUX_BLOCK_REQUEST_HANDLER_FACTORY_H