#ifndef MEMORYMUX_HEARTBEAT_SERVICE_HANDLER_H
#define MEMORYMUX_HEARTBEAT_SERVICE_HANDLER_H

#include "heartbeat_service.h"
#include "../block/block_allocator.h"
#include "../directory_ops.h"
#include "health_metadata.h"

namespace mmux {
namespace directory {

class heartbeat_service_handler: public heartbeat_serviceIf {
 public:
  typedef std::map<std::string, health_metadata> health_metadata_map;

  heartbeat_service_handler(std::shared_ptr<block_allocator> alloc, std::shared_ptr<directory_management_ops> mgmt, std::shared_ptr<std::map<std::string, health_metadata>> hm_map);

  void ping(const heartbeat &hb) override;

 private:
  std::shared_ptr<block_allocator> alloc_;
  std::shared_ptr<directory_management_ops> mgmt_;
  std::shared_ptr<std::map<std::string, health_metadata>> hm_map_;
};

}
}

#endif //MEMORYMUX_HEARTBEAT_SERVICE_HANDLER_H
