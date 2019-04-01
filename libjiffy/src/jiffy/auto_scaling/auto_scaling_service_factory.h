#ifndef JIFFY_AUTO_SCALING_RPC_SERVICE_FACTORY_H
#define JIFFY_AUTO_SCALING_RPC_SERVICE_FACTORY_H

#include "jiffy/storage/block.h"
#include "auto_scaling_service.h"

namespace jiffy {
namespace auto_scaling {
/* Storage management service factory class
 * Inherited from storage management_serviceIfFactory class */
class auto_scaling_service_factory : public auto_scaling_serviceIfFactory {
 public:

  /**
   * @brief Constructor
   * @param blocks Blocks
   */

  explicit auto_scaling_factory(std::vector<std::shared_ptr<block>> &blocks);

  /**
   * @brief Fetch storage management service handler
   * @param connInfo Connection information
   * @return Handler
   */

  auto_scaling_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;

  /**
   * @brief Release handler
   * @param anIf Handler to be released
   */

  void releaseHandler(auto_scaling_serviceIf *anIf) override;
 private:

  /* Chain blocks */
  std::vector<std::shared_ptr<block>> &blocks_;
};

}
}

#endif //JIFFY_AUTO_SCALING_RPC_SERVICE_FACTORY_H
