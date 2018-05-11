#ifndef MMUX_DIRECTORY_RPC_SERVICE_FACTORY_H
#define MMUX_DIRECTORY_RPC_SERVICE_FACTORY_H

#include "directory_service.h"
#include "directory_tree.h"

namespace mmux {
namespace directory {

class directory_service_factory : public directory_serviceIfFactory {
 public:
  explicit directory_service_factory(std::shared_ptr<directory_tree> shard);
  directory_serviceIf *getHandler(const ::apache::thrift::TConnectionInfo &connInfo) override;
  void releaseHandler(directory_serviceIf *anIf) override;
 private:
  std::shared_ptr<directory_tree> shard_;
};

}
}

#endif //MMUX_DIRECTORY_RPC_SERVICE_FACTORY_H
