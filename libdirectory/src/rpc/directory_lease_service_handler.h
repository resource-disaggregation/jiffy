#ifndef ELASTICMEM_DIRECTORY_LEASE_SERVICE_HANDLER_H
#define ELASTICMEM_DIRECTORY_LEASE_SERVICE_HANDLER_H

#include "directory_lease_service.h"
#include "../shard/directory_service_shard.h"
namespace elasticmem {
namespace directory {

class directory_lease_service_handler : public directory_lease_serviceIf {
 public:
  explicit directory_lease_service_handler(std::shared_ptr<directory_service_shard> shard);

  void create(const std::string &path) override;
  void load(const std::string &persistent_path, const std::string &memory_path) override;
  void renew_lease(rpc_keep_alive_ack &_return, const rpc_keep_alive &msg) override;
  void remove(const std::string &path, rpc_remove_mode mode) override;

 private:
  std::shared_ptr<directory_service_shard> shard_;
};

}
}

#endif //ELASTICMEM_DIRECTORY_LEASE_SERVICE_HANDLER_H
