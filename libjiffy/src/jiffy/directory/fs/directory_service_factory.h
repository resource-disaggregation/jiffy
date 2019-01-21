#ifndef JIFFY_DIRECTORY_RPC_SERVICE_FACTORY_H
#define JIFFY_DIRECTORY_RPC_SERVICE_FACTORY_H

#include "directory_service.h"
#include "directory_tree.h"

namespace jiffy {
namespace directory {

/**
 * Directory service factory class
 * Inherited from directory_serviceIfFactory class
 */
class directory_service_factory : public directory_serviceIfFactory {
 public:

  /**
   * @brief Constructor
   * @param shard Directory tree
   */

  explicit directory_service_factory(std::shared_ptr<directory_tree> shard);

  /**
   * @brief Get directory_service_handler
   * @param conn_info Connection information
   * @return Directory service handler
   */

  directory_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;

  /**
   * @brief Delete the service handler
   * @param handler Directory service handler
   */

  void releaseHandler(directory_serviceIf *anIf) override;

 private:
  /* Directory tree */
  std::shared_ptr<directory_tree> shard_;
};

}
}

#endif //JIFFY_DIRECTORY_RPC_SERVICE_FACTORY_H
