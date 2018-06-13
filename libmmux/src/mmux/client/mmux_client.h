#ifndef MMUX_MMUX_CLIENT_H
#define MMUX_MMUX_CLIENT_H

#include <string>
#include "../directory/directory_ops.h"
#include "../directory/client/lease_renewal_worker.h"
#include "../storage/client/kv_client.h"
#include "../storage/client/kv_listener.h"

namespace mmux {
namespace client {

class mmux_client {
 public:
  mmux_client(const std::string &host, int dir_port, int lease_port);

  std::shared_ptr<directory::directory_ops> fs();
  directory::lease_renewal_worker &lease_worker();

  void begin_scope(const std::string &path);
  void end_scope(const std::string &path);

  storage::kv_client create(const std::string &path,
                            const std::string &persistent_store_prefix,
                            size_t num_blocks = 1,
                            size_t chain_length = 1);
  storage::kv_client open(const std::string &path);
  storage::kv_client open_or_create(const std::string &path,
                                    const std::string &persistent_store_prefix,
                                    size_t num_blocks = 1,
                                    size_t chain_length = 1);
  storage::kv_listener listen(const std::string &path);
  void remove(const std::string &path);
  void flush(const std::string &path, const std::string &dest);

 private:
  std::shared_ptr<directory::directory_ops> fs_;
  directory::lease_renewal_worker lease_worker_;
};

}
}

#endif //MMUX_MMUX_CLIENT_H
