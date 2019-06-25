#ifndef JIFFY_AUTO_SCALING_RPC_SERVICE_HANDLER_H
#define JIFFY_AUTO_SCALING_RPC_SERVICE_HANDLER_H

#include "auto_scaling_service.h"
#include "jiffy/storage/client/replica_chain_client.h"

namespace jiffy {
namespace auto_scaling {
/* Auto scaling service handler class */
class auto_scaling_service_handler : public auto_scaling_serviceIf {
 public:
  /**
   * @brief Constructor
   * @param directory_host Directory server host name
   * @param directory_port Directory server port number
   */

  explicit auto_scaling_service_handler(const std::string directory_host, int directory_port);

  /**
   * @brief Auto scaling handling function
   * @param current_replica_chain Current replica chain
   * @param path Path
   * @param conf Configuration map
   */
  void auto_scaling(const std::vector<std::string> &current_replica_chain,
                    const std::string &path,
                    const std::map<std::string, std::string> &conf) override;

 private:
  /**
   * @brief Make exceptions
   * @param e exception
   * @return Auto scaling exceptions
   */

  auto_scaling_exception make_exception(std::exception &e);

  /**
   * @brief Make exceptions
   * @param msg Exception message
   * @return Auto scaling exceptions
   */

  auto_scaling_exception make_exception(const std::string &msg);
  /* Directory server host name */
  std::string directory_host_;
  /* Directory server port number */
  int directory_port_;


};

}
}

#endif //JIFFY_AUTO_SCALING_RPC_SERVICE_HANDLER_H
