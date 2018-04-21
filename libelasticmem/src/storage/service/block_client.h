#ifndef ELASTICMEM_KV_CLIENT_H
#define ELASTICMEM_KV_CLIENT_H

#include <thrift/transport/TSocket.h>
#include "block_request_service.h"
#include "block_response_service.h"
#include <libcuckoo/cuckoohash_map.hh>
#include <future>

namespace elasticmem {
namespace storage {

class block_client {
 public:
  typedef cuckoohash_map<int64_t, std::shared_ptr<std::promise<std::string>>> promise_map;

  class command_response_handler : public block_response_serviceIf {
   public:
    explicit command_response_handler(promise_map& promises): promises_(promises) {}
    void response(const sequence_id &seq, const std::vector<std::string> &result) override {
      promises_.find(seq.client_seq_no)->set_value(result.at(0));
    }
   private:
    promise_map& promises_;
  };

  typedef block_request_serviceClient thrift_client;

  block_client() = default;
  ~block_client();
  int64_t get_client_id();
  void connect(const std::string &hostname, int port, int block_id);
  void disconnect();
  bool is_connected();

  std::thread add_response_listener(int64_t client_id, promise_map &promises);
  void command_request(const sequence_id &seq,
                       int32_t cmd_id,
                       const std::vector<std::string> &arguments);


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
