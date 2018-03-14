#ifndef ELASTICMEM_KV_CLIENT_H
#define ELASTICMEM_KV_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "../kv_management_service.h"
#include "kv_rpc_service.h"
namespace elasticmem {
namespace kv {

class kv_client {
 public:
  typedef kv_rpc_serviceClient thrift_client;

  kv_client() = default;
  ~kv_client();
  kv_client(const std::string &hostname, int port, int block_id);
  void connect(const std::string &hostname, int port);
  void disconnect();

  void put(const key_type &key, const value_type &value);

  value_type get(const key_type &key);

  void update(const key_type &key, const value_type &value);

  void remove(const key_type &key);

  std::size_t size() const;

  bool empty() const;

 private:
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  std::shared_ptr<thrift_client> client_{};
  int block_id_;
};

}
}

#endif //ELASTICMEM_KV_CLIENT_H
