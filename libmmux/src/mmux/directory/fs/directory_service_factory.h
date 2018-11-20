#ifndef MMUX_DIRECTORY_RPC_SERVICE_FACTORY_H
#define MMUX_DIRECTORY_RPC_SERVICE_FACTORY_H

#include "directory_service.h"
#include "directory_tree.h"

namespace mmux {
namespace directory {

/**
 * Directory service class
 * Inherited from directory_serviceIfFactory
 */
class directory_service_factory : public directory_serviceIfFactory {
 public:

  /**
   * @brief Construction function of directory_service_factory class
   * @param shard Server's directory tree
   */

  explicit directory_service_factory(std::shared_ptr<directory_tree> shard);

  /**
   * @brief Get directory_service_handler
   * @param conn_info Connection info provided by apache thrift
   * @return Directory_service_handler generated with directory tree
   */

  directory_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;

  /**
   * @brief Delete the directory_service_handler
   * @param handler Directory_service_handler
   */

  void releaseHandler(directory_serviceIf *anIf) override;

 private:
  /* Directory tree */
  std::shared_ptr<directory_tree> shard_;
};

}
}

#endif //MMUX_DIRECTORY_RPC_SERVICE_FACTORY_H
