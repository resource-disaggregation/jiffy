#ifndef JIFFY_SHARED_LOG_CLIENT_H
#define JIFFY_SHARED_LOG_CLIENT_H

#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/client_cache.h"
#include "jiffy/storage/shared_log/shared_log_ops.h"
#include "jiffy/storage/client/data_structure_client.h"

namespace jiffy {
namespace storage {

class shared_log_client : public data_structure_client {
 public:
  /**
   * @brief Constructor
   * Store all replica chain and their begin slot
   * @param fs Directory service
   * @param path Key value block path
   * @param status Data status
   * @param timeout_ms Timeout
   */
  shared_log_client(std::shared_ptr<directory::directory_interface> fs,
              const std::string &path,
              const directory::data_status &status,
              int timeout_ms = 1000);

  /**
   * @brief Destructor
   */
  virtual ~shared_log_client() = default;

  /**
   * @brief Refresh the slot and blocks from directory service
   */
  void refresh() override;

  /**
   * @brief Read data from shared_log
   * @param buf Buffer
   * @param size Size
   * @return Read status, -1 if reach EOF, number of bytes read otherwise
   */
  int scan(std::vector<std::string> &buf, const std::string &start_pos, const std::string &end_pos, const std::vector<std::string> &logical_streams);

  /**
   * @brief Write data to shared_log
   * @param data Data
   * @return Number of bytes written, or -1 if blocks are insufficient
   */
  int write(const std::string &position, const std::string &data_, const std::vector<std::string> &logical_streams);

  /**
   * @brief Seek to a location of the shared_log
   * @param offset shared_log offset to seek
   * @return Boolean, true if offset is within shared_log range
   */
  bool trim(const std::string &start_pos, const std::string &end_pos);

  /**
   * @brief Handle command in redirect case
   * @param _return Response to be collected
   * @param args Command args
   */

  void handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) override;

 protected:

  /**
   * @brief Check if new chain needs to be added
   * @param op Operation
   * @return Boolean, true if new chain needs to be added
   */
  bool need_chain() const;

  /**
   * @brief Fetch block identifier for specified operation
   * @param op Operation
   * @return Block identifier
   */
  std::size_t block_id() const;

  /**
   * @brief Track the last partition of the shared_log
   */
  void update_last_partition() {
    if (last_partition_ < cur_partition_) {
      last_partition_ = cur_partition_;
      last_offset_ = cur_offset_;
    }
  }

  /**
   * @brief Update last offset of the shared_log
   */
  void update_last_offset() {
    if (last_offset_ < cur_offset_ && cur_partition_ == last_partition_)
      last_offset_ = cur_offset_;
  }

  /* Current partition number */
  std::size_t cur_partition_;
  /* Current offset in a partition */
  std::size_t cur_offset_;
  /* Last partition of the shared_log */
  std::size_t last_partition_;
  /* Max partition of the shared_log */
  std::size_t max_partition_;
  /* Last offset of the shared_log */
  std::size_t last_offset_;
  /* Replica chain clients, each partition only save a replica chain client */
  std::vector<std::shared_ptr<replica_chain_client>> blocks_;
  /* Block size */
  std::size_t block_size_;
  /* Auto scaling support */
  bool auto_scaling_;
};

}
}

#endif //JIFFY_shared_log_CLIENT_H
