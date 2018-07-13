#ifndef MMUX_KV_MANAGEMENT_RPC_CLIENT_H
#define MMUX_KV_MANAGEMENT_RPC_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "storage_management_service.h"
#include "../chain_module.h"

namespace mmux {
namespace storage {

class storage_management_client {
 public:
  typedef storage_management_serviceClient thrift_client;
  storage_management_client() = default;
  ~storage_management_client();
  storage_management_client(const std::string &host, int port);
  void connect(const std::string &host, int port);
  void disconnect();

  void setup_block(int32_t block_id,
                   const std::string &path,
                   int32_t slot_begin,
                   int32_t slot_end,
                   const std::vector<std::string> &chain,
                   bool auto_scale,
                   int32_t role,
                   const std::string &next_block_name);
  std::pair<int32_t, int32_t> slot_range(int32_t block_id);
  void set_exporting(int32_t block_id,
                     const std::vector<std::string> &target_block_name,
                     int32_t slot_begin,
                     int32_t slot_end);
  void set_importing(int32_t block_id, int32_t slot_begin, int32_t slot_end);
  void setup_and_set_importing(int32_t block_id,
                               const std::string &path,
                               int32_t slot_begin,
                               int32_t slot_end,
                               const std::vector<std::string> &chain,
                               int32_t role,
                               const std::string &next_block_name);
  void set_regular(int32_t block_id, int32_t slot_begin, int32_t slot_end);
  std::string path(int32_t block_id);
  void sync(int32_t block_id, const std::string &backing_path);
  void load(int32_t block_id, const std::string &backing_path);
  void dump(int32_t block_id, const std::string &backing_path);
  void reset(int32_t block_id);
  int64_t storage_capacity(int32_t block_id);
  int64_t storage_size(int32_t block_id);
  void resend_pending(int32_t block_id);
  void forward_all(int32_t block_id);
  void export_slots(int32_t block_id);

 private:
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //MMUX_KV_MANAGEMENT_RPC_CLIENT_H
