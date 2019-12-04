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

  /**
   * @brief Destructor
   */
  virtual ~fifo_queue_client() = default;

  /**
   * @brief Refresh the slot and blocks from directory service
   */
  void refresh() override;

  /**
   * @brief Enqueue message
   * @param item New item
   */
  void enqueue(const std::string &item);

  /**
   * @brief Dequeue item
   */
  void dequeue();

  /**
   * @brief Read next item without dequeue
   * @return Read next result
   */
  std::string read_next();

  /**
   * @brief Fetch Queue size
   * @return Queue size
   */
  std::size_t length();

  /**
   * @brief Fetch in rate of the queue
   * @return Queue in rate
   */
  double in_rate();

  /**
   * @brief Fetch out rate of the queue
   * @return Queue out rate
   */
  double out_rate();

  /**
   * @brief Fetch the front element of the queue
   * @return Dequeue result
   */
  std::string front();
 private:
  /**
   * @brief Handle command in redirect case
   * @param args Arguments
   * @param _return Response
   */
  void handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) override;

  /**
   * @brief Handle partition identifier changes after auto scaling
   * @param args Arguments
   */
  void handle_partition_id(const std::vector<std::string> &args);

  /**
   * @brief Run command repeatedly
   * @param _return Response
   * @param args Arguments
   */
  void run_repeated(std::vector<std::string> &_return, const std::vector<std::string> &args);

  /**
   * @brief Fetch block identifier for specific command
   * @param args Arguments
   * @return Block identifier
   */
  std::size_t block_id(const std::vector<std::string> &args);

  /**
   * @brief Add next data block due to auto scaling
   * @param _return Response
   * @param args Arguments
   */
  void add_blocks(const std::vector<std::string> &_return, const std::vector<std::string> &args);

  /* Dequeue partition id */
  std::size_t dequeue_partition_;

  /* Enqueue partition id */
  std::size_t enqueue_partition_;

  /* Replica chain clients, each partition only save a replica chain client */
  std::vector<std::shared_ptr<replica_chain_client>> blocks_;

  /* Read next partition */
  std::size_t read_partition_;

  /* Starting name of the chains */
  std::size_t start_;

  /* Boolean, true if using auto scaling */
  bool auto_scaling_;

};

}
}

#endif //JIFFY_FIFO_QUEUE_CLIENT_H
