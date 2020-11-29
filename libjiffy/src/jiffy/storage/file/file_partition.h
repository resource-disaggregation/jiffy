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
   * @brief Write data to the file
   * @param _return Response
   * @param args Arguments
   */
  void write(response &_return, const arg_list &args);

  /**
   * @brief Read data from the file
   * @param _return Response
   * @param args Arguments
   */
  void read(response &_return, const arg_list &args);

  /**
   * @brief Clear the file
   * @param _return Response
   * @param args Arguments
   */
  void clear(response &_return, const arg_list &args);

  /**
   * @brief Update partition
   * @param _return Response
   * @param args Arguments
   */
  void update_partition(response &_return, const arg_list &args);

  /**
   * @brief Add blocks
   * @param _return Response
   * @param args Arguments
   */
  void add_blocks(response &_return, const arg_list &args);

  /**
   * @brief Get storage capacity of the partition
   * @param _return Response
   * @param args Arguments
   */
  void get_storage_capacity(response &_return, const arg_list &args);

  /**
   * @brief Get size of the partition
   * @param _return Response
   * @param args Arguments
   */
  void get_partition_size(response &_return, const arg_list &args);

  /**
   * @brief Run command on file partition
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

 private:

  

  /* File partition */
  file_type partition_;

  /* Custom serializer/deserializer */
  std::shared_ptr<serde> ser_;

  /* Bool for partition slot range splitting */
  bool scaling_up_;

  /* Partition dirty bit */
  bool dirty_;

  /* Bool to indicate if block is successfully allocated */
  bool block_allocated_;

  /* Bool value for auto scaling */
  bool auto_scale_;

  /* Auto scaling server hostname */
  std::string auto_scaling_host_;

  /* Auto scaling server port number */
  int auto_scaling_port_;

  std::vector<std::string> allocated_blocks_;

  std::shared_ptr<persistent::persistent_service> remote_;

};

}
}

#endif //JIFFY_FILE_SERVICE_SHARD_H
