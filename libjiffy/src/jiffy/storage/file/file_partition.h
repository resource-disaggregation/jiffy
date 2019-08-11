#ifndef JIFFY_FILE_SERVICE_SHARD_H
#define JIFFY_FILE_SERVICE_SHARD_H

#include <string>
#include <jiffy/utils/property_map.h>
#include "../serde/serde_all.h"
#include "jiffy/storage/partition.h"
#include "jiffy/persistent/persistent_service.h"
#include "jiffy/storage/chain_module.h"
#include "file_defs.h"
#include "jiffy/directory/directory_ops.h"

namespace jiffy {
namespace storage {

class file_partition : public chain_module {
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
  explicit file_partition(block_memory_manager *manager,
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
  ~file_partition() override = default;

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
   * @brief Write data to the file
   * @param _return Response
   * @param args Arguments
   */
  void write(response& _return, const arg_list &args);

  /**
   * @brief Read data from the file
   * @param _return Response
   * @param args Arguments
   */
  void read(response& _return, const arg_list &args);

  /**
   *@brief Fetch the metadata for seek
   * @param _return Response
   * @param args Arguments
   */
  void seek(response& _return, const arg_list &args);

  /**
   * @brief Clear the file
   * @param _return Response
   * @param args Arguments
   */
  void clear(response& _return, const arg_list &args);

  /**
   * @brief Update partition with next partition pointer
   * @param _return Response
   * @param args Arguments
   */
  void update_partition(response& _return, const arg_list &args);

  /**
   * @brief Run command on file partition
   * @param _return Response
   * @param args Arguments
   */
  void run_command(response &_return, const arg_list &args) override;

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
   * @param target_str Next target replica chain in string format
   */
  void next_target(const std::string &target_str) {
    next_target_str_ = target_str;
  }

  /**
   * @brief Fetch next target string
   * @return Next target string
   */
  std::string next_target() const {
    return next_target_str_;
  }

 private:

  /**
   * @brief Check if block is overloaded
   * @return Bool value, true if block size is over the high threshold capacity
   */
  bool overload();

  /* File partition */
  file_type partition_;

  /* Custom serializer/deserializer */
  std::shared_ptr<serde> ser_;

  /* High threshold */
  double threshold_hi_;

  /* Bool for partition slot range splitting */
  bool scaling_up_;

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
  std::string next_target_str_;

};

}
}

#endif //JIFFY_FILE_SERVICE_SHARD_H
