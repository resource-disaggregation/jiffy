#ifndef JIFFY_CHAIN_REQUEST_HANDLER_FACTORY_H
#define JIFFY_CHAIN_REQUEST_HANDLER_FACTORY_H

#include "chain_request_service.h"
#include "jiffy/storage/block.h"

namespace jiffy {
namespace storage {
/* Chain request handler factory
 * Inherited from chain request serviceIfFactory */
class chain_request_handler_factory: public chain_request_serviceIfFactory {
 public:
  /**
   * @brief Constructor
   * @param blocks Data blocks
   */

  explicit chain_request_handler_factory(std::vector<std::shared_ptr<block>> &blocks);

  /**
   * @brief Fetch chain request handler
   * @param connInfo Connection information
   * @return Chain request handler
   */

  chain_request_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;

  /**
   * @brief Release chain request handler
   * @param anIf Chain request handler
   */

  void releaseHandler(chain_request_serviceIf *anIf) override;

 private:
  /* Data blocks */
  std::vector<std::shared_ptr<block>> &blocks_;
};

}
}

#endif //JIFFY_CHAIN_REQUEST_HANDLER_FACTORY_H
