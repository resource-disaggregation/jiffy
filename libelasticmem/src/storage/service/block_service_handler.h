#ifndef ELASTICMEM_KV_RPC_SERVICE_HANDLER_H
#define ELASTICMEM_KV_RPC_SERVICE_HANDLER_H

#include "block_service.h"
#include "../notification/subscription_map.h"
#include "../chain_module.h"

namespace elasticmem {
namespace storage {

class block_service_handler : public block_serviceIf {
 public:
  block_service_handler(std::vector<std::shared_ptr<chain_module>> &blocks,
                        std::vector<std::shared_ptr<subscription_map>> &sub_maps);
  void run_command(std::vector<std::string> &_return,
                   int32_t block_id,
                   int32_t cmd_id,
                   const std::vector<std::string> &arguments) override;
 private:
  block_exception make_exception(const std::exception &ex);

  std::vector<std::shared_ptr<chain_module>> &blocks_;
  std::vector<std::shared_ptr<subscription_map>> &sub_maps_;
};

}
}

#endif //ELASTICMEM_KV_RPC_SERVICE_HANDLER_H
