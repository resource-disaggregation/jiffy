#ifndef MMUX_MMUX_CLIENT_H
#define MMUX_MMUX_CLIENT_H

#include <string>
#include "../directory/client/directory_client.h"
#include "../directory/client/lease_renewal_worker.h"
#include "../storage/client/kv_client.h"

namespace mmux {
namespace client {

class mmux_client {
 public:
  mmux_client(const std::string &host, int dir_port, int lease_port);

  void begin_scope(const std::string &path);
  void end_scope(const std::string &path);

  storage::kv_client create(const std::string &path,
                            const std::string &persistent_store_prefix,
                            size_t num_blocks,
                            size_t chain_length);
  storage::kv_client open(const std::string &path);
  storage::kv_client open_or_create(const std::string &path,
                                    const std::string &persistent_store_prefix,
                                    size_t num_blocks,
                                    size_t chain_length);
  void remove(const std::string &path);
  void flush(const std::string &path);

 private:
  directory::directory_client fs_;
  directory::lease_renewal_worker lease_worker_;
};

}
}

#endif //MMUX_MMUX_CLIENT_H
