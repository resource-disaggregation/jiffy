#ifndef MMUX_LOCAL_STORE_H
#define MMUX_LOCAL_STORE_H

#include "../persistent_service.h"

namespace mmux {
namespace persistent {

class local_store : public persistent_service {
 public:
  void write(const std::string &local_path, const std::string &remote_path) override;

  void read(const std::string &remote_path, const std::string &local_path) override;

  void remove(const std::string &remote_path) override;
};

}
}

#endif //MMUX_LOCAL_STORE_H
