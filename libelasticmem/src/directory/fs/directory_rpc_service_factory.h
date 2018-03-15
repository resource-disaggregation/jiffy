#ifndef ELASTICMEM_DIRECTORY_RPC_SERVICE_FACTORY_H
#define ELASTICMEM_DIRECTORY_RPC_SERVICE_FACTORY_H

#include "directory_rpc_service.h"
#include "directory_tree.h"

namespace elasticmem {
namespace directory {

class directory_rpc_service_factory : public directory_rpc_serviceIfFactory {
 public:
  explicit directory_rpc_service_factory(std::shared_ptr<directory_tree> shard);
  directory_rpc_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;
  void releaseHandler(directory_rpc_serviceIf *anIf) override;
 private:
  std::shared_ptr<directory_tree> shard_;
};

}
}

#endif //ELASTICMEM_DIRECTORY_RPC_SERVICE_FACTORY_H
