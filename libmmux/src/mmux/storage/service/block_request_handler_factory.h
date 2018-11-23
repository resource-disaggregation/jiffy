#ifndef MMUX_BLOCK_REQUEST_HANDLER_FACTORY_H
#define MMUX_BLOCK_REQUEST_HANDLER_FACTORY_H

#include "block_request_service.h"
#include "../chain_module.h"

namespace mmux {
namespace storage {
/* */
class block_request_handler_factory : public block_request_serviceIfFactory {
 public:
  /**
   * @brief
   * @param blocks
   */

  explicit block_request_handler_factory(std::vector<std::shared_ptr<chain_module>> &blocks);

  /**
   * @brief
   * @param connInfo
   * @return
   */

  block_request_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;

  /**
   * @brief
   * @param anIf
   */

  void releaseHandler(block_request_serviceIf *anIf) override;

 public:
  /* */
  std::vector<std::shared_ptr<chain_module>> &blocks_;
  /* */
  std::atomic<int64_t> client_id_gen_;
};

}
}

#endif //MMUX_BLOCK_REQUEST_HANDLER_FACTORY_H
