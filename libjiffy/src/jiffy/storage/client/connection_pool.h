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
  typedef std::string response_t;
  typedef blocking_queue<response_t> mailbox_t;
  connection_pool(int timeout_ms = 1000) {
    timeout_ms_ = timeout_ms;
  }
  void init(std::vector<std::string> block_ids, int timeout_ms = 1000);

  connection_instance request_connection(block_id & block_info);

  void release_connection(std::size_t block_id);



 private:
  std::map<std::size_t, bool> free_;
  std::mutex mutex_;
  int timeout_ms_;
//  response_worker worker_;
  mailbox_t response_;
  std::size_t pool_size_;
  std::vector<std::string> block_ids_;
  /* Map from service port to connection_instanse*/
  cuckoohash_map<int, connection_instance> connections_; // TODO this connection should be per block server instead of per block id

};

}
}

#endif //JIFFY_CONNECTION_POOL_H
