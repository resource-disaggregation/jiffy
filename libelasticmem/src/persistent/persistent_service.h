#ifndef ELASTICMEM_PERSISTENT_STORE_H
#define ELASTICMEM_PERSISTENT_STORE_H

#include <string>

namespace elasticmem {
namespace persistent {

class persistent_service {
 public:
  virtual ~persistent_service() = default;

  virtual void write(const std::string &local_path, const std::string &remote_path) = 0;

  virtual void read(const std::string &remote_path, const std::string &local_path) = 0;

  virtual void remove(const std::string &remote_path) = 0;
};

}
}

#endif //ELASTICMEM_PERSISTENT_STORE_H