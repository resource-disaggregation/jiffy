#ifndef JIFFY_FIFO_QUEUE_SERVICE_SHARD_H
#define JIFFY_FIFO_QUEUE_SERVICE_SHARD_H

#include <string>
#include <jiffy/utils/property_map.h>
#include "../serde/serde_all.h"
#include "jiffy/storage/partition.h"
#include "jiffy/persistent/persistent_service.h"
#include "jiffy/storage/chain_module.h"
#include "fifo_queue_defs.h"
#include "jiffy/directory/directory_ops.h"

namespace jiffy {
namespace storage {
/* Fifo queue partition class */
class fifo_queue_partition : public chain_module {
 public:

  /**
   * @brief Constructor
   * @param manager Block memory manager
   * @param name Partition name
   * @param metadata Partition metadata
   * @param conf Property map
   * @param directory_host Directory server host name
   * @param directory_port Directory server port number
   * @param auto_scaling_host Auto scaling server host name
   * @param auto_scaling_port Auto scaling server port number
   */
  explicit fifo_queue_partition(block_memory_manager *manager,
                                const std::string &name = "0",
                                const std::string &metadata = "regular",
                                const utils::property_map &conf = {},
                                const std::string &directory_host = "localhost",
                                int directory_port = 9091,
                                const std::string &auto_scaling_host = "localhost",
                                int auto_scaling_port = 9095);

  /**
   * @brief Virtual destructor
   */
  virtual ~fifo_queue_partition() = default;

  /**
   * @brief Fetch block size
   * @return Block size
   */

  std::size_t size() const;

  /**
   * @brief Check if block is empty
   * @return Bool value, true if empty
   */

  bool empty() const;

  /**
   * @brief Enqueue a new message to the fifo queue
   * @param message New message
   * @return Enqueue return status string
   */
  std::string enqueue(const std::string &message);

  /**
   * @brief Dequeue a new message from the fifo queue
   * @return Dequeue return status string
   */
  std::string dequeue();

  /**
   * @brief Read next message without dequeue
   * @param Read next position
   * @return Read next return status string
   */
  std::string readnext(std::string pos);

  /**
   * @brief Clear the fifo queue
   * @return Clear return status
   */
  std::string clear();

  /**
   * @brief Update partition with next target partition
   * @param next_target Next target partition string
   * @return Update status string
   */
  std::string update_partition(const std::string &next_target);

  /**
   * @brief Run particular command on key value block
   * @param _return Return status to be collected
   * @param cmd_id Operation identifier
   * @param args Command arguments
   */

  void run_command(std::vector<std::string> &_return, int cmd_id, const std::vector<std::string> &args) override;

  /**
   * @brief Atomically check dirty bit
   * @return Bool value, true if block is dirty
   */

  bool is_dirty() const;

  /**
   * @brief Load persistent data into the block
   * @param path Persistent storage path
   */

  void load(const std::string &path) override;

  /**
   * @brief If dirty, synchronize persistent storage and block
   * @param path Persistent storage path
   * @return Bool value, true if block successfully synchronized
   */

  bool sync(const std::string &path) override;

  /**
   * @brief Flush the block if dirty and clear the block
   * @param path Persistent storage path
   * @return Bool value, true if block successfully dumped
   */

  bool dump(const std::string &path) override;

  /**
   * @brief Send all key and value to the next block
   */

  void forward_all() override;

  /**
   * @brief Set next target string
   * @param block_ids Next target replica chain data blocks
   */
  void next_target(std::vector<std::string> &target) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    next_target_string = "";
    for (const auto &block: target) {
      next_target_string += (block + "!");
    }
    next_target_string.pop_back();
  }

  /**
   * @brief Set next target string
   * @param target_str Next target string
   */
  void next_target(const std::string &target_str) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    next_target_string = target_str;
  }
  /**
   * @brief Fetch next target string
   * @return Next target string
   */
  std::string next_target_str() {
    return next_target_string;
  }

 private:

  /**
   * @brief Check if block is overloaded
   * @return Bool value, true if block size is over the high threshold capacity
   */

  bool overload();

  /* Fifo queue partition */
  fifo_queue_type partition_;

  /* Custom serializer/deserializer */
  std::shared_ptr<serde> ser_;

  /* High threshold */
  double threshold_hi_;

  /* Bool for overload partition */
  bool overload_;

  /* Bool for underload partition */
  bool underload_;

  /* Bool for new block available, this bool basically prevents the fifo queue to erase all the blocks when size = 0 */
  bool new_block_available_;

  /* Partition dirty bit */
  bool dirty_;

  /* Bool value for auto scaling */
  bool auto_scale_;

  /* Directory server hostname */
  std::string directory_host_;

  /* Directory server port number */
  int directory_port_;

  /* Auto scaling server hostname */
  std::string auto_scaling_host_;
  
  /* Auto scaling server port number */
  int auto_scaling_port_;

  /* Next partition target string */
  std::string next_target_string;

  /* Head position of queue */
  std::size_t head_;

};

}
}

#endif //JIFFY_FIFO_QUEUE_SERVICE_SHARD_H
