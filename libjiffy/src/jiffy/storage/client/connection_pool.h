#ifndef JIFFY_CONNECTION_POOL_H
#define JIFFY_CONNECTION_POOL_H


#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/client/block_client.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/client_cache.h"
#include "jiffy/storage/manager/detail/block_id_parser.h"
#include "jiffy/storage/client/response_worker.h"

namespace jiffy {
namespace storage {

struct connection_instance {
  std::shared_ptr<block_client> connection;
  std::size_t client_id;
};


class connection_pool {
 public:
  typedef std::string response_t;
  typedef blocking_queue<response_t> mailbox_t;
  connection_pool() = default;
  void init(std::vector<std::string> block_ids, int timeout_ms_ = 1000);

  connection_instance & request_connection(std::size_t block_id);

  void release_connection(std::size_t block_id);



 private:
  std::vector<bool> free_;
  int timeout_ms_;
//  response_worker worker_;
  mailbox_t response_;
  std::size_t pool_size_;
  std::vector<connection_instance> connections_;
  std::vector<std::string> block_ids_;

};

}
}

#endif //JIFFY_CONNECTION_POOL_H
