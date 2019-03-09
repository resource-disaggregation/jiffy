#ifndef JIFFY_MSG_QUEUE_CLIENT_H
#define JIFFY_MSG_QUEUE_CLIENT_H

#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/client_cache.h"
#include "jiffy/storage/msgqueue/msg_queue_ops.h"
#include "jiffy/storage/client/data_structure_client.h"

namespace jiffy {
namespace storage {


class msg_queue_client : data_structure_client {
 public:
  /**
   * @brief Constructor
   * Store all replica chain and their begin slot
   * @param fs Directory service
   * @param path Key value block path
   * @param status Data status
   * @param timeout_ms Timeout
   */

  msg_queue_client(std::shared_ptr<directory::directory_interface> fs,
                   const std::string &path,
                   const directory::data_status &status,
                   int timeout_ms = 1000);

  virtual ~msg_queue_client() = default;
  /**
   * @brief Refresh the slot and blocks from directory service
   */

  void refresh() override;

  /**
   * @brief Send message
   * @param msg New message
   * @return Response of the command
   */

  std::string send(const std::string &msg);

  /**
   * @brief Read message at the end position
   * @return Response of the command
   */

  std::string read();

  /**
   * @brief Send message in batch
   * @param msgs New messages
   * @return Response of the commands
   */

  std::vector<std::string> send(const std::vector<std::string> &msgs);

  /**
   * @brief Read message in batch
   * @param num_msg Number of message to be read in batch
   * @return Response of batch command
   */

  std::vector<std::string> read(std::size_t num_msg);

 private:
  /**
   * @brief Get the read start position and increase it by one
   * @return Start position in string
   */
  std::string get_inc_read_pos() {
    auto old_val = read_start_;
    read_start_++;
    return std::to_string(old_val);
  }

  /**
   * @brief Fetch block identifier for particular key
   * @param key Key
   * @return Block identifier
   */

  size_t block_id(const std::string &key);

  /**
   * @brief Run same operation in batch
   * @param id Operation identifier
   * @param args Operation arguments
   * @param args_per_op Argument per operation
   * @return
   */

  std::vector<std::string> batch_command(const msg_queue_cmd_id &id,
                                         const std::vector<std::string> &args,
                                         size_t args_per_op);

  /* Read start */
  std::size_t read_start_;
  /* Read End */
  std::size_t read_end_;   // TODO add usage


};

}
}

#endif //JIFFY_MSG_QUEUE_CLIENT_H