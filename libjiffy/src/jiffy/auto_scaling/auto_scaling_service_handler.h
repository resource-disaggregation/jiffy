#ifndef JIFFY_AUTO_SCALING_RPC_SERVICE_HANDLER_H
#define JIFFY_AUTO_SCALING_RPC_SERVICE_HANDLER_H

#include "auto_scaling_service.h"
#include "jiffy/storage/client/replica_chain_client.h"

namespace jiffy {
namespace auto_scaling {
/* Storage management service handler class,
 * inherited from storage_management_serviceIf */
class auto_scaling_service_handler : public auto_scaling_serviceIf {
 public:

  /**
   * @brief Constructor
   * @param blocks Blocks
   */

  explicit auto_scaling_service_handler(const std::string directory_host, int directory_port);

  void auto_scaling(const std::vector<std::string> & current_replica_chain, const std::string& path, const std::map<std::string, std::string> & conf) override;

 private:

  /**
   * @brief Make exceptions
   * @param e exception
   * @return Storage management exceptions
   */

  auto_scaling_exception make_exception(std::exception &e);

  /**
   * @brief Make exceptions
   * @param msg Exception message
   * @return Storage management exceptions
   */

  auto_scaling_exception make_exception(const std::string &msg);

  std::string directory_host_;
  int directory_port_;

};

}
}

#endif //JIFFY_AUTO_SCALING_RPC_SERVICE_HANDLER_H
