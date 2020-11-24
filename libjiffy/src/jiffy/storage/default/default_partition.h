#ifndef JIFFY_DEFAULT_SERVICE_SHARD_H
#define JIFFY_DEFAULT_SERVICE_SHARD_H

#include <string>
#include <jiffy/utils/property_map.h>
#include "jiffy/storage/partition.h"
#include "jiffy/storage/chain_module.h"

namespace jiffy {
namespace storage {

class default_partition : public chain_module {
 public:

  /**
   * @brief Constructor
   * @param manager Block memory manager
   * @param name Partition name
   * @param metadata Partition metadata
   * @param conf Partition property map
   * @param directory_host Directory server host name
   * @param directory_port Directory server port number
   * @param auto_scaling_host Auto scaling server host name
   * @param auto_scaling_port Auto scaling server port number
   */
  explicit default_partition(block_memory_manager *manager,
                               const std::string &name = "default",
                               const std::string &metadata = "default",
                               const utils::property_map &conf = {},
                               const std::string &auto_scaling_host = "localhost",
                               int auto_scaling_port = 9095);

  /**
   * @brief Virtual destructor
   */
  ~default_partition() override = default;

  /**
   * @brief Run particular command on key value block
   * @param _return Return status to be collected
   * @param args Command arguments
   */

  void run_command_impl(response &_return, const arg_list &args) override;
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
};
}
}

#endif //JIFFY_DEFAULT_SERVICE_SHARD_H
