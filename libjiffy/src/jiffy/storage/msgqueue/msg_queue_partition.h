#ifndef JIFFY_MSG_QUEUE_SERVICE_SHARD_H
#define JIFFY_MSG_QUEUE_SERVICE_SHARD_H

#include <string>
#include <jiffy/utils/property_map.h>
#include "../serde/serde_all.h"
#include "jiffy/storage/partition.h"
#include "jiffy/persistent/persistent_service.h"
#include "jiffy/storage/chain_module.h"
#include "msg_queue_defs.h"
#include "jiffy/directory/directory_ops.h"

namespace jiffy {
namespace storage {

class msg_queue_partition : public chain_module {
 public:

  explicit msg_queue_partition(block_memory_manager *manager,
                               const std::string &name = "0", //TODO need to fix this name
                               const std::string &metadata = "regular", //TODO need to fix this metadata
                               const utils::property_map &conf = {},
                               const std::string &directory_host = "localhost",
                               const int directory_port = 9091);

  /**
   * @brief Virtual destructor
   */
  virtual ~msg_queue_partition() = default;

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
   * @brief Send a new message to the message queue
   * @param message New message
   * @return Send return status string
   */
  std::string send(const std::string &message);

  /**
   * @brief Read a new message from the message queue
   * @return Read return status string
   */
  std::string read(std::string position);

  /**
   * @brief Clear the message queue
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
   * @brief Load persistent data into the block, lock the block while doing this
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
    for(const auto &block: target) {
      next_target_string += (block + "!");
    }
    next_target_string.pop_back();
  }

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

  /* Message queue partition */
  msg_queue_type partition_;

  /* Custom serializer/deserializer */
  std::shared_ptr<serde> ser_;

  /* Low threshold */
  double threshold_lo_;
  /* High threshold */
  double threshold_hi_;

  /* Atomic bool for partition slot range splitting */
  std::atomic<bool> overload_;

  /* Atomic partition dirty bit */
  std::atomic<bool> dirty_;

  /* Bool value for auto scaling */
  std::atomic_bool auto_scale_;

  /* Directory server hostname */
  std::string directory_host_;

  /* Directory server port number */
  int directory_port_;

  /* Next partition target string */
  std::string next_target_string;

};

}
}

#endif //JIFFY_MSG_QUEUE_SERVICE_SHARD_H
