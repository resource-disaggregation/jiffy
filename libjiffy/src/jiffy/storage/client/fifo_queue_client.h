#ifndef JIFFY_FIFO_QUEUE_CLIENT_H
#define JIFFY_FIFO_QUEUE_CLIENT_H

#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/client_cache.h"
#include "jiffy/storage/fifoqueue/fifo_queue_ops.h"
#include "jiffy/storage/client/data_structure_client.h"
#include "jiffy/storage/fifoqueue/string_array.h"

namespace jiffy {
namespace storage {

class fifo_queue_client : data_structure_client {
 public:
  /**
   * @brief Constructor
   * @param fs Directory service
   * @param path Key value block path
   * @param status Data status
   * @param timeout_ms Timeout
   */

  fifo_queue_client(std::shared_ptr<directory::directory_interface> fs,
                    const std::string &path,
                    const directory::data_status &status,
                    int timeout_ms = 1000);

  virtual ~fifo_queue_client() = default;
  /**
   * @brief Refresh the slot and blocks from directory service
   */

  void refresh() override;

  /**
   * @brief Enqueue message
   * @param msg New message
   * @return Enqueue result
   */

  std::string enqueue(const std::string &msg);

  /**
   * @brief Dequeue message
   * @return Dequeue result
   */

  std::string dequeue();

  /**
   * @brief Read next message without dequeue
   * @return Read next result
   */ 
  std::string readnext();

 private:

  /**
   * @brief Check if new chain needs to be added
   * @param op Operation
   * @return Boolean, true if new chain needs to be added
   */
  bool add_chain(const fifo_queue_cmd_id &op);
  /**
   * @brief Fetch block identifier for specified operation
   * @param op Operation
   * @return Block identifier
   */

  std::size_t block_id(const fifo_queue_cmd_id &op);

  /**
   * @brief Check if partition number is valid
   * @param partition_num Partition number
   * @return Boolean, true if valid
   */
  bool check_valid_id(std::size_t partition_num) {
    if(partition_num < blocks_.size())
      return true;
    else return false;
  }
  /**
   * @brief Handle command in redirect case
   * @param cmd_id Command identifier
   * @param response Response to be collected
   */

  void handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) override;

  /**
   * @brief Handle multiple commands in redirect case
   * @param cmd_id Command identifier
   * @param args Command arguments
   * @param responses Responses to be collected
   */

  void handle_redirects(int32_t cmd_id,
                        std::vector<std::string> &args,
                        std::vector<std::string> &responses) override;

  /* Dequeue partition id */
  std::size_t dequeue_partition_;
  /* Enqueue partition id */
  std::size_t enqueue_partition_;
  /* Replica chain clients, each partition only save a replica chain client */
  std::vector<std::shared_ptr<replica_chain_client>> blocks_;
  /* Read offset */
  std::size_t read_offset_;
  /* Read next partition */
  std::size_t read_partition_;
   
};

}
}

#endif //JIFFY_FIFO_QUEUE_CLIENT_H