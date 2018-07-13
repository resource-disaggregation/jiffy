#ifndef MEMORYMUX_HEARTBEAT_SERVICE_FACTORY_H
#define MEMORYMUX_HEARTBEAT_SERVICE_FACTORY_H

#include "heartbeat_service.h"
#include "../block/block_allocator.h"
#include "../directory_ops.h"
#include "health_metadata.h"

namespace mmux {
namespace directory {

class heartbeat_service_factory: public heartbeat_serviceIfFactory {
 public:
  explicit heartbeat_service_factory(std::shared_ptr<block_allocator> alloc,
                                     std::shared_ptr<directory_management_ops> mgmt,
                                     std::shared_ptr<std::map<std::string, health_metadata>> hm_map);
  heartbeat_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;
  void releaseHandler(heartbeat_serviceIf *anIf) override;

 private:
  std::shared_ptr<block_allocator> alloc_;
  std::shared_ptr<directory_management_ops> mgmt_;
  std::shared_ptr<std::map<std::string, health_metadata>> hm_map_;
};

}
}

#endif //MEMORYMUX_HEARTBEAT_SERVICE_FACTORY_H
