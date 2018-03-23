#ifndef ELASTICMEM_KV_CLIENT_H
#define ELASTICMEM_KV_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "../storage_management_service.h"
#include "block_service.h"
#include "../block/kv/kv_block.h"

namespace elasticmem {
namespace storage {

class block_client {
 public:
  typedef block_serviceClient thrift_client;

  block_client() = default;
  ~block_client();
  block_client(const std::string &hostname, int port, int block_id);
  void connect(const std::string &hostname, int port, int block_id);
  void disconnect();
  bool is_connected();

  void run_command(std::vector<std::string>& _return, int op_id, const std::vector<std::string>& args);

  void put(const key_type &key, const value_type &value);

  value_type get(const key_type &key);

  void update(const key_type &key, const value_type &value);

  void remove(const key_type &key);

 private:
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  std::shared_ptr<thrift_client> client_{};
  int block_id_{};
};

}
}

#endif //ELASTICMEM_KV_CLIENT_H
