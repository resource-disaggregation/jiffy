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

  explicit default_partition(block_memory_manager *manager,
                               const std::string &name = "default",
                               const std::string &metadata = "default",
                               const utils::property_map &conf = {},
                               const std::string &directory_host = "localhost",
                               const int directory_port = 9091,
                               const std::string &auto_scaling_host = "localhost",
                               const int auto_scaling_port = 9095);

  /**
   * @brief Virtual destructor
   */
  virtual ~default_partition() = default;

  /**
   * @brief Run particular command on key value block
   * @param _return Return status to be collected
   * @param cmd_id Operation identifier
   * @param args Command arguments
   */

  void run_command(std::vector<std::string> &_return, int cmd_id, const std::vector<std::string> &args) override;
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
