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
#include "jiffy/utils/time_utils.h"

namespace jiffy {
namespace storage {
enum fifo_queue_size_type : uint32_t {
  head_size = 0,
  tail_size = 1
};
/* Fifo queue partition class */
class fifo_queue_partition : public chain_module {
 public:

  /**
   * @brief Constructor
   * @param manager Block memory manager
   * @param name Partition name
   * @param metadata Partition metadata
   * @param conf Property map
   * @param auto_scaling_host Auto scaling server host name
   * @param auto_scaling_port Auto scaling server port number
   */
  explicit fifo_queue_partition(block_memory_manager *manager,
                                const std::string &name = "0",
                                const std::string &metadata = "regular",
                                const utils::property_map &conf = {},
                                const std::string &auto_scaling_host = "localhost",
                                int auto_scaling_port = 9095);

  /**
   * @brief Virtual destructor
   */
  ~fifo_queue_partition() override = default;

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
   * @brief Enqueue a new item to the fifo queue
   * @param item New message
   * @return Enqueue return status string
   */
  void enqueue(response &_return, const arg_list &args);

  /**
   * @brief Dequeue an item from the fifo queue
   * @param _return Response
   * @param args Arguments
   */
  void dequeue(response &_return, const arg_list &args);

  /**
   * @brief Fetch an item without dequeue
   * @param _return Response
   * @param args Arguments
   */
  void read_next(response &_return, const arg_list &args);

  /**
   * @brief Clear the fifo queue
   * @param _return Response
   * @param args Arguments
   */
  void clear(response &_return, const arg_list &args);

  /**
   * @brief Update partition with next partition pointer
   * @param _return Response
   * @param args Arguments
   */
  void update_partition(response &_return, const arg_list &args);

  /**
   * @brief Fetch number of elements of the queue
   * @param _return Response
   * @param args Arguments
   */
  void length(response &_return, const arg_list &args);

  /**
   * @brief Fetch in rate of the queue
   * @param _return Response
   * @param args Arguments
   */
  void in_rate(response &_return, const arg_list &args);

  /**
   * @brief Fetch out rate of the queue
   * @param _return Response
   * @param args Arguments
   */
  void out_rate(response &_return, const arg_list &args);

  /**
   * @brief Fetch the front element of the queue
   * @param _return Response
   * @param args Arguments
   */
  void front(response &_return, const arg_list &args);

  /**
   * @brief Run particular command on fifo queue partition
   * @param _return Response
   * @param args Arguments
   */
  void run_command_impl(response &_return, const arg_list &args) override;

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
   * @param target_str Next target string
   */
  void next_target(const std::string &target_str) {
    next_target_str_ = target_str;
  }

 private:

  /**
   * @brief Check if block is overloaded
   * @return Bool value, true if block size is over the high threshold capacity
   */
  bool overload();

  /**
   * @brief Check if block is underloaded
   * @return Bool value, true if block is empty
   */
  bool underload();

  /**
   * @brief Update read head pointer
   * @return Bool value, true if pointer updated
   */
  bool update_read_head() {
    if (read_head_ < head_) {
      read_head_ = head_;
      return true;
    }
    return false;
  }

  /**
   * @brief Update in rate and out rate
   */
  void update_rate();

  /**
   * @brief Clear the partition
   */
  void clear_partition();

  /* Fifo queue partition */
  fifo_queue_type partition_;

  /* Custom serializer/deserializer */
  std::shared_ptr<serde> ser_;

  /* Bool for overload partition */
  bool scaling_up_;

  /* Bool for underload partition */
  bool scaling_down_;

  /* Partition dirty bit */
  bool dirty_;

  /* Bool value for auto scaling */
  bool auto_scale_;

  /* Auto scaling server hostname */
  std::string auto_scaling_host_;

  /* Auto scaling server port number */
  int auto_scaling_port_;

  /* Next partition target string */
  std::string next_target_str_;

  /* Head position of queue */
  std::size_t head_;

  /* Head position for read next operation */
  std::size_t read_head_;

  /* Boolean indicating whether enqueues are redirected to the next chain */
  bool enqueue_redirected_;

  /* Boolean indicating whether dequeues are redirected to the next chain */
  bool dequeue_redirected_;

  /* Boolean indicating whether readnexts are redirected to the next chain */
  bool readnext_redirected_;

  /* Number of elements inserted in the queue */
  std::size_t enqueue_data_size_;

  /* Number of elements removed from the queue */
  std::size_t dequeue_data_size_;

  /* Total number of elements from all of the previous partitions */
  std::size_t prev_data_size_;

  /* Enqueue element start */
  std::size_t enqueue_start_data_size_;

  /* Dequeue element start */
  std::size_t dequeue_start_data_size_;

  /* Enqueue start time */
  std::size_t enqueue_start_time_;

  /* Dequeue start time */
  std::size_t dequeue_start_time_;

  /* Enqueue time counter */
  std::size_t enqueue_time_count_;

  /* Dequeue time counter */
  std::size_t dequeue_time_count_;

  /* In rate */
  double in_rate_;

  /* Out rate */
  double out_rate_;

  /* Boolean indicating if in rate is set */
  bool in_rate_set_;

  /* Boolean indicating if out rate is set */
  bool out_rate_set_;

  /* Periodicity for rate calculation in microseconds */
  std::size_t periodicity_us_;

};

}
}

#endif //JIFFY_FIFO_QUEUE_SERVICE_SHARD_H
