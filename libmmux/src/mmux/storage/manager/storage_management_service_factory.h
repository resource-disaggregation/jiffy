#ifndef MMUX_KV_MANAGEMENT_RPC_SERVICE_FACTORY_H
#define MMUX_KV_MANAGEMENT_RPC_SERVICE_FACTORY_H

#include "storage_management_service.h"
#include "../block.h"
#include "../chain_module.h"

namespace mmux {
namespace storage {
/* Storage management service factory class
 * Inherited from storage management_serviceIfFactory class */
class storage_management_service_factory : public storage_management_serviceIfFactory {
 public:

  /**
   * @brief Constructor
   * @param blocks Blocks
   */

  explicit storage_management_service_factory(std::vector<std::shared_ptr<chain_module>> &blocks);

  /**
   * @brief Fetch storage management service handler
   * @param connInfo Connection information
   * @return Handler
   */

  storage_management_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;

  /**
   * @brief Release handler
   * @param anIf Handler to be released
   */

  void releaseHandler(storage_management_serviceIf *anIf) override;
 private:

  /* Chain blocks */
  std::vector<std::shared_ptr<chain_module>> &blocks_;
};

}
}

#endif //MMUX_KV_MANAGEMENT_RPC_SERVICE_FACTORY_H