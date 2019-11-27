#ifndef JIFFY_CONNECTION_POOL_H
#define JIFFY_CONNECTION_POOL_H


#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/client/pool_block_client.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/client_cache.h"
#include "jiffy/storage/manager/detail/block_id_parser.h"
#include "jiffy/storage/client/response_worker.h"
//#include <libcuckoo/cuckoohash_map.hh>
#include "../../../../../thirdparty/libcuckoo/libcuckoo/cuckoohash_map.hh"

namespace jiffy {
namespace storage {
typedef std::vector<std::string> response_t;
typedef blocking_queue<response_t> mailbox_t;

struct connection_instance {
  std::shared_ptr<pool_block_client> connection;
  int64_t client_id;
  int32_t block_id;
  bool free_;
  pool_block_client::command_response_reader response_reader;
  bool in_flight_;
  std::shared_ptr<mailbox_t> response_mailbox;
  connection_instance() {
    free_ = false;
    in_flight_ = false;
    connection = std::make_shared<pool_block_client>();
  }
};

class connection_pool {
 public:


  struct register_info {
    std::string address;
    std::size_t offset;
    int64_t client_id;
    register_info() = default;
    register_info(std::string &_address, std::size_t _offset, int64_t _client_id) {
      address = _address;
      offset = _offset;
      client_id = _client_id;
    }
  };

  explicit connection_pool(std::size_t pool_size = 100, int timeout_ms = 1000) {
    pool_size_ = pool_size;
    timeout_ms_ = timeout_ms;
  }

  ~connection_pool();

  void init(std::vector<std::string> block_ids, int timeout_ms = 1000);

  register_info request_connection(block_id & block_info);

  void release_connection(register_info &connection_info);

  void send_run_command(register_info connection_info, const int32_t block_id, const std::vector<std::string> &arguments);

  void recv_run_command(register_info connection_info, std::vector<std::string> &_return);

  void command_request(register_info connection_info, const sequence_id &seq, const std::vector<std::string> &args);

  int64_t recv_response(register_info connection_info, std::vector<std::string> &out);

  void get_command_response_reader(register_info connection_info, int64_t client_id);



 private:
  std::mutex mutex_;
  int timeout_ms_;
  //response_worker worker_; One worker is inefficient
  std::vector<std::shared_ptr<response_worker>> workers_;
  std::size_t pool_size_;
  std::vector<std::string> block_ids_;
  /* Map from service port to connection_instanse*/
  cuckoohash_map<std::string, std::vector<connection_instance>> connections_; // TODO this connection should be per block server instead of per block id, and a block server is determined by host and port
  cuckoohash_map<std::string, mailbox_t> queue;
};

}
}

#endif //JIFFY_CONNECTION_POOL_H
