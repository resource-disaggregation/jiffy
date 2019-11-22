#ifndef JIFFY_CONNECTION_POOL_H
#define JIFFY_CONNECTION_POOL_H


#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/client/block_client.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/client_cache.h"
#include "jiffy/storage/manager/detail/block_id_parser.h"
#include "jiffy/storage/client/response_worker.h"
#include <libcuckoo/cuckoohash_map.hh>

namespace jiffy {
namespace storage {

struct connection_instance {
  std::shared_ptr<block_client> connection;
  int64_t client_id;
  block_client::command_response_reader response_reader;
  connection_instance() {
    connection = std::make_shared<block_client>();
  }
};


class connection_pool {
 public:
  struct register_info {
    int service_port;
    int offset;
    int64_t client_id;
    register_info() = default;
    register_info(int _port, int _offset, int64_t _client_id) {
      service_port = _port;
      offset = _offset;
      client_id = _client_id;
    }
  };
  typedef std::string response_t;
  typedef blocking_queue<response_t> mailbox_t;
  connection_pool(int timeout_ms = 1000) {
    timeout_ms_ = timeout_ms;
  }
  void init(std::vector<std::string> block_ids, int timeout_ms = 1000);

  register_info request_connection(block_id & block_info);

  void release_connection(std::size_t block_id);

  void send_run_command(register_info connection_info, const int32_t block_id, const std::vector<std::string> &arguments);

  void recv_run_command(register_info connection_info, std::vector<std::string> &_return);

  void command_request(register_info connection_info, const sequence_id &seq, const std::vector<std::string> &args);

  int64_t recv_response(register_info connection_info, std::vector<std::string> &out);

  void get_command_response_reader(register_info connection_info, int64_t client_id);



 private:
  std::map<std::size_t, bool> free_;
  std::mutex mutex_;
  int timeout_ms_;
  //response_worker worker_;
  mailbox_t response_;
  std::size_t pool_size_;
  std::vector<std::string> block_ids_;
  /* Map from service port to connection_instanse*/
  cuckoohash_map<int, std::vector<connection_instance>> connections_; // TODO this connection should be per block server instead of per block id

};

}
}

#endif //JIFFY_CONNECTION_POOL_H
