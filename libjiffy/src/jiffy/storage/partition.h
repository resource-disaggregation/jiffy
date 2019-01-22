#ifndef JIFFY_BLOCK_H
#define JIFFY_BLOCK_H

#include <utility>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <stdexcept>
#include <iostream>
#include <shared_mutex>
#include "notification/subscription_map.h"
#include "service/block_response_client_map.h"
#include "command.h"

namespace jiffy {
namespace storage {

#define DEFAULT_CAPACITY 134217728

class memory_block;

/* Partition class */
class partition {
 public:
  /**
   * @brief Constructor
   * @param name Partition name
   * @param metadata Partition metadata
   * @param supported_commands Commands supported by the partition
   */

  explicit partition(const std::string &name,
                     const std::string &metadata,
                     const std::vector<command> &supported_commands);

  /**
   * @brief Destructor
   */
  virtual ~partition() = default;

  /**
   * @brief Virtual function for running a command on a block
   * @param _return Return value
   * @param cmd_id Operation identifier
   * @param args Operation arguments
   */

  virtual void run_command(std::vector<std::string> &_return,
                           int32_t cmd_id,
                           const std::vector<std::string> &args) = 0;

  /**
   * @brief Set block path
   * @param path Block path
   */

  void path(const std::string &path);

  /**
   * @brief Fetch block path
   * @return Block path
   */

  const std::string &path() const;

  /**
   * @brief Fetch partition name
   * @return Partition name
   */

  const std::string &name() const;

  /**
   * @brief Fetch partition metadata
   * @return Partition metadata
   */
  const std::string &metadata() const;

  /**
   * @brief Check if ith command type is accessor
   * @param i Command identifier
   * @return Bool value, true if block is accessor
   */

  bool is_accessor(int i) const;

  /**
   * @brief Check if ith command  type is mutator
   * @param i Command identifier
   * @return Bool value, true if is mutator
   */

  bool is_mutator(int i) const;

  /**
   * @brief Fetch command name
   * @param cmd_id Command identifier
   * @return Operation name
   */

  std::string command_name(int cmd_id);

  /**
   * Management Operations
   * Virtual function
   */

  virtual void load(const std::string &path) = 0;

  /**
   * @brief Synchronize partition with persistent store.
   * @param path Persistent store path to write to.
   * @return True if data was written, false otherwise.
   */
  virtual bool sync(const std::string &path) = 0;

  /**
   * @brief Dump partition data to persistent store.
   * @param path Persistent store path to write to.
   * @return True if data was written, false otherwise.
   */
  virtual bool dump(const std::string &path) = 0;

  /**
   * @brief Get the storage capacity of the partition.
   * @return The storage capacity of the partition.
   */
  std::size_t storage_capacity();

  /**
   * @brief Get the storage utilized by the partition.
   * @return The storage capacity utilized by the partition.
   */
  std::size_t storage_size();

  /**
   * @brief Fetch subscription map
   * @return Subscription map
   */

  subscription_map &subscriptions();

  /**
   * @brief Fetch block response client map
   * @return Block response client map
   */

  block_response_client_map &clients();

 protected:
  /* Metadata mutex */
  mutable std::shared_mutex metadata_mtx_;
  /* Partition name */
  std::string name_;
  /* Partition metadata */
  std::string metadata_;
  /* Partition path */
  std::string path_;
  /* Supported commands */
  const std::vector<command> &supported_commands_;
  /* Atomic value to collect the sum of key size and value size */
  std::atomic<size_t> bytes_;
  /* Key value partition capacity */
  std::size_t capacity_;
  /* Subscription map */
  subscription_map sub_map_{};
  /* Block response client map */
  block_response_client_map client_map_{};

 private:
  void set_capacity(size_t capacity);

  friend class memory_block;
};

}
}
#endif //JIFFY_BLOCK_H
