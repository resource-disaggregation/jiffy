#ifndef JIFFY_FILE_CLIENT_H
#define JIFFY_FILE_CLIENT_H

#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/client_cache.h"
#include "jiffy/storage/file/file_ops.h"
#include "jiffy/storage/client/data_structure_client.h"

//#define MAX(a,b) \
//   ({ __typeof__ (a) _a = (a); \
//       __typeof__ (b) _b = (b); \
//     _a > _b ? _a : _b; })
//
//#define MIN(a,b) \
//   ({ __typeof__ (a) _a = (a); \
//       __typeof__ (b) _b = (b); \
//     _a < _b ? _a : _b; })

namespace jiffy {
namespace storage {

class file_client : public data_structure_client {
 public:
  /**
   * @brief Constructor
   * Store all replica chain and their begin slot
   * @param fs Directory service
   * @param path Key value block path
   * @param status Data status
   * @param timeout_ms Timeout
   */

  file_client(std::shared_ptr<directory::directory_interface> fs,
              const std::string &path,
              const directory::data_status &status,
              int timeout_ms = 1000);

  /**
   * @brief Destructor
   */
  virtual ~file_client() = default;

  /**
   * @brief Refresh the slot and blocks from directory service
   */
  void refresh() override;

  /**
   * @brief Seek to a location of the file
   * @param offset File offset to seek
   * @return Boolean, true if offset is within file range
   */
  bool seek(std::size_t offset);

 protected:

  /**
   * @brief Check if new chain needs to be added
   * @param op Operation
   * @return Boolean, true if new chain needs to be added
   */
  bool need_chain() const;

  /**
   * @brief Check the number of the new chains to be added
   * @param data_size Data to be written
   * @return Number of new chains to be added
   */
  std::size_t num_chain(std::size_t data_size) const;

  /**
   * @brief Fetch block identifier for specified operation
   * @param op Operation
   * @return Block identifier
   */
  std::size_t block_id() const;

  /**
   * @brief Track the last partition of the file
   * @param partition Partition
   */
  void update_last_partition(std::size_t partition) {
    if (last_partition_ < partition)
      last_partition_ = partition;
  }
  /**
   * @brief Fetch last partition number
   * @return Last partition number
   */
  std::size_t last_partition() {
    return last_partition_;
  }

  /* Current partition number */
  std::size_t cur_partition_;
  /* Current offset in a partition */
  std::size_t cur_offset_;
  /* Last partition of the file */
  std::size_t last_partition_;
  /* Replica chain clients, each partition only save a replica chain client */
  std::vector<std::shared_ptr<replica_chain_client>> blocks_;
  /* Block size */
  std::size_t block_size_;
};

}
}

#endif //JIFFY_FILE_CLIENT_H
