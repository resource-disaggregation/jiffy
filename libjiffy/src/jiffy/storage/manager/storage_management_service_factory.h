#ifndef JIFFY_KV_MANAGEMENT_RPC_SERVICE_FACTORY_H
#define JIFFY_KV_MANAGEMENT_RPC_SERVICE_FACTORY_H

#include "jiffy/storage/block.h"
#include "storage_management_service.h"

namespace jiffy {
namespace storage {
/* Storage management service factory class
 * Inherited from storage management_serviceIfFactory class */
class storage_management_service_factory : public storage_management_serviceIfFactory {
 public:

  /**
   * @brief Constructor
   * @param blocks Blocks
   */

  explicit storage_management_service_factory(std::vector<std::shared_ptr<block>> &blocks);

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
  std::vector<std::shared_ptr<block>> &blocks_;
};

}
}

#endif //JIFFY_KV_MANAGEMENT_RPC_SERVICE_FACTORY_H
