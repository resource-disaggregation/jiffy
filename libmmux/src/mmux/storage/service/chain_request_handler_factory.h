#ifndef MMUX_CHAIN_REQUEST_HANDLER_FACTORY_H
#define MMUX_CHAIN_REQUEST_HANDLER_FACTORY_H

#include "chain_request_service.h"
#include "../chain_module.h"

namespace mmux {
namespace storage {
/* */
class chain_request_handler_factory: public chain_request_serviceIfFactory {
 public:
  /**
   * @brief
   * @param blocks
   */

  explicit chain_request_handler_factory(std::vector<std::shared_ptr<chain_module>> &blocks);

  /**
   * @brief
   * @param connInfo
   * @return
   */

  chain_request_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;

  /**
   * @brief
   * @param anIf
   */

  void releaseHandler(chain_request_serviceIf *anIf) override;

 private:
  /* */
  std::vector<std::shared_ptr<chain_module>> &blocks_;
};

}
}

#endif //MMUX_CHAIN_REQUEST_HANDLER_FACTORY_H
