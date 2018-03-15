#ifndef ELASTICMEM_LOCAL_STORE_H
#define ELASTICMEM_LOCAL_STORE_H

#include "../persistent_service.h"

namespace elasticmem {
namespace persistent {

class local_store : public persistent_service {
 public:
  void write(const std::string &local_path, const std::string &remote_path) override;

  void read(const std::string &remote_path, const std::string &local_path) override;

  void remove(const std::string &remote_path) override;
};

}
}

#endif //ELASTICMEM_LOCAL_STORE_H
