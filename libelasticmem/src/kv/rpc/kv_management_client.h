#ifndef ELASTICMEM_KV_MANAGEMENT_RPC_CLIENT_H
#define ELASTICMEM_KV_MANAGEMENT_RPC_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "kv_management_rpc_service.h"

namespace elasticmem {
namespace kv {

class kv_management_client {
 public:
  typedef kv_management_rpc_serviceClient thrift_client;
  kv_management_client() = default;
  ~kv_management_client();
  kv_management_client(const std::string &hostname, int port);
  void connect(const std::string &hostname, int port);
  void disconnect();

  void flush(int32_t block_id, const std::string &persistent_store_prefix, const std::string &path);
  void load(int32_t block_id, const std::string &persistent_store_prefix, const std::string &path);
  void clear(int32_t block_id);
  int64_t storage_capacity(int32_t block_id);
  int64_t storage_size(int32_t block_id);

 private:
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  std::shared_ptr<thrift_client> client_{};
};

}
}

#endif //ELASTICMEM_KV_MANAGEMENT_RPC_CLIENT_H
