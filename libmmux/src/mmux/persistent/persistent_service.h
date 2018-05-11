#ifndef MMUX_PERSISTENT_STORE_H
#define MMUX_PERSISTENT_STORE_H

#include <string>

namespace mmux {
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

#endif //MMUX_PERSISTENT_STORE_H
