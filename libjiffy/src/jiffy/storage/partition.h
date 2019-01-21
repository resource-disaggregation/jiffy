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


// TODO: Setting metadata should be atomic: e.g., reset function, or setup function, should acquire lock before
// setting metadata
/* Partition class */
class partition {
 public:
  /**
   * @brief Constructor
   * @param supported_commands Commands supported by the partition
   * @param name Partition name
   */

  explicit partition(const std::vector<command> &supported_commands, std::string name);

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
   * @brief Fetch block name
   * @return Block name
   */

  const std::string &name() const;

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

  virtual bool sync(const std::string &path) = 0;

  virtual bool dump(const std::string &path) = 0;

  virtual std::size_t storage_capacity() = 0;

  virtual std::size_t storage_size() = 0;

  virtual void reset() = 0;

  virtual void export_slots() = 0;

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
  /* Block operations */
  const std::vector<command> &supported_commands_;
  /* Block file path */
  std::string path_;
  /* Block name */
  std::string name_;
  /* Partition name */
  std::string partition_name_;
  /* Partition metadata */
  std::string partition_metadata_;
  /* Subscription map */
  subscription_map sub_map_{};
  /* Block response client map */
  block_response_client_map client_map_{};
};

}
}
#endif //JIFFY_BLOCK_H
