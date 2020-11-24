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
#include <jiffy/storage/types/binary.h>
#include "jiffy/storage/notification/subscription_map.h"
#include "jiffy/storage/service/block_response_client_map.h"
#include "jiffy/storage/command.h"
#include "jiffy/storage/block_memory_manager.h"
#include "jiffy/storage/block_memory_allocator.h"
#include "jiffy/utils/logger.h"

#define RETURN(...)           \
  _return = { __VA_ARGS__ };  \
  return

#define RETURN_OK(...) RETURN("!ok", ##__VA_ARGS__)
#define RETURN_ERR(...) RETURN(__VA_ARGS__)

namespace jiffy {
namespace storage {

using namespace utils;

typedef std::vector<std::string> arg_list;
typedef std::vector<std::string> response;

/* Partition class */
class partition {
 public:
  template<typename T>
  using allocator = block_memory_allocator<T>;

  /**
   * @brief Constructor
   * @param name Partition name
   * @param metadata Partition metadata
   * @param supported_commands Commands supported by the partition
   */

  explicit partition(block_memory_manager *manager,
                     const std::string &name,
                     const std::string &metadata,
                     const command_map &supported_commands);

  /**
   * @brief Destructor
   */
  virtual ~partition() {
    client_map_.send_failure();
    client_map_.clear();
    sub_map_.clear();
  }

  /**
   * @brief Virtual function for running a command on a block
   * @param _return Return value
   * @param cmd_id Operation identifier
   * @param args Operation arguments
   */
  virtual void run_command_impl(response &_return, const arg_list &args) = 0;

  void run_command(response &_return, const arg_list &args);

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
   * @brief Set partition name
   * @param name Partition name
   */
  void name(const std::string &name);

  /**
   * @brief Fetch partition name
   * @return Partition name
   */
  const std::string &name() const;

  /**
   * @brief Set partition metadata
   * @param metadata Partition metadata
   */
  void metadata(const std::string &metadata);

  /**
   * @brief Fetch partition metadata
   * @return Partition metadata
   */
  const std::string &metadata() const;

  /**
   * @brief Check if ith command type is accessor
   * @param cmd Command name
   * @return Bool value, true if block is accessor
   */
  bool is_accessor(const std::string& cmd) const;

  /**
   * @brief Check if ith command  type is mutator
   * @param cmd Command name
   * @return Bool value, true if is mutator
   */
  bool is_mutator(const std::string& cmd) const;

  /**
   * @brief Fetch command id
   * @param cmd_name Name of the command
   * @return Command ID
   */
  uint32_t command_id(const std::string& cmd_name);

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
   * @brief Build STL compliant allocator with memory managed by block memory manager.
   * @tparam T Type of data.
   * @return STL compliant allocator.
   */
  template<typename T>
  allocator<T> build_allocator() {
    return allocator<T>(manager_);
  }

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

  /**
   * @brief Set partition name and partition metadata
   * @param name New partition name
   * @param metadata New partition metadata
   */
  void set_name_and_metadata(const std::string &name, const std::string &metadata);

  /**
   * @brief Notify the listener
   * @param args Arguments
   */
  void notify(const arg_list & args);

  // Return underlying block sequence number
  // Increases monotonically over lifetime of block
  int32_t seq_no();

  void update_seq_no(int32_t x);

 protected:
  /**
   * @brief Construct binary string
   * @param str String
   * @return Binary string
   */
  binary make_binary(const std::string &str);

  int32_t extract_block_seq_no(const arg_list &args, arg_list &out_list);

  /* Partition name */
  std::string name_;
  /* Partition metadata */
  std::string metadata_;
  /* Partition path */
  std::string path_;
  /* Supported commands */
  const command_map &supported_commands_;
  /* Subscription map */
  subscription_map sub_map_{};
  /* Block response client map */
  block_response_client_map client_map_{};
  /* Block memory manager */
  block_memory_manager *manager_;
  /* Binary allocator */
  allocator<uint8_t> binary_allocator_;
  /* Atomic bool to indicate that the partition is a default one */
  std::atomic<bool> default_{};

  // Block sequence number
  int32_t block_seq_no_;
};

}
}
#endif //JIFFY_BLOCK_H
