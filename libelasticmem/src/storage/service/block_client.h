#ifndef ELASTICMEM_KV_CLIENT_H
#define ELASTICMEM_KV_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "../storage_management_ops.h"
#include "block_service.h"

namespace elasticmem {
namespace storage {

class block_client {
 public:
  typedef block_serviceConcurrentClient thrift_client;

  block_client() = default;
  ~block_client();
  block_client(const std::string &hostname, int port, int block_id);
  void connect(const std::string &hostname, int port, int block_id);
  void disconnect();
  bool is_connected();

  void run_command(std::vector<std::string> &_return,
                     int64_t seq_no,
                     int32_t op_id,
                     const std::vector<std::string> &args);

  int32_t send_command(int64_t seq_no, int32_t op_id, const std::vector<std::string> &args);

  void recv_command_result(int32_t seq_id, std::vector<std::string>& _return);

  void put(const std::string &key, const std::string &value);

  std::string get(const std::string &key);

  void update(const std::string &key, const std::string &value);

  void remove(const std::string &key);

  std::string endpoint() const;

 private:
  std::shared_ptr<apache::thrift::transport::TSocket> socket_{};
  std::shared_ptr<apache::thrift::transport::TTransport> transport_{};
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_{};
  std::shared_ptr<thrift_client> client_{};
  std::string host_;
  int port_{-1};
  int block_id_{-1};
};

}
}

#endif //ELASTICMEM_KV_CLIENT_H
