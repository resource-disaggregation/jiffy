#ifndef JIFFY_AUTO_SCALING_RPC_SERVICE_FACTORY_H
#define JIFFY_AUTO_SCALING_RPC_SERVICE_FACTORY_H

#include "auto_scaling_service.h"

namespace jiffy {
namespace auto_scaling {
/* Auto scaling service factory class */
class auto_scaling_service_factory : public auto_scaling_serviceIfFactory {
 public:
  /**
   * @brief Constructor
   * @param directory_host Directory server host name
   * @param directory_port Directory server port number
   */
  explicit auto_scaling_service_factory(const std::string directory_host, int directory_port);

  /**
   * @brief Fetch auto scaling service handler
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
  /* Directory host name */
  std::string directory_host_;
  /* Directory port number */
  int directory_port_;

};

}
}

#endif //JIFFY_AUTO_SCALING_RPC_SERVICE_FACTORY_H
