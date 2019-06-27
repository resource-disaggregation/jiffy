#ifndef JIFFY_FILE_CLIENT_H
#define JIFFY_FILE_CLIENT_H

#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/client_cache.h"
#include "jiffy/storage/file/file_ops.h"
#include "jiffy/storage/client/data_structure_client.h"

namespace jiffy {
namespace storage {

class file_client : data_structure_client {
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

  virtual ~file_client() = default;
  /**
   * @brief Refresh the slot and blocks from directory service
   */

  void refresh() override;

  /**
   * @brief Write message to file
   * @param msg New message
   * @return Response of the command
   */

  std::string write(const std::string &msg);

  /**
   * @brief Read message from file
   * @param size Size to be read
   * @return Response of the command
   */

  std::string read(const std::size_t size);

  /**
   * @brief Seek to a location of the file
   * @param offset File offset to seek
   * @return Boolean, true if offset is within file range
   */
  bool seek(const std::size_t offset); 

 private:

  /**
   * @brief Check if new chain needs to be added
   * @param op Operation
   * @return Boolean, true if new chain needs to be added
   */ 
  bool need_chain(const file_cmd_id &op) const;
  /**
   * @brief Fetch block identifier for specified operation
   * @param op Operation
   * @return Block identifier
   */

  std::size_t block_id(const file_cmd_id &op) const;

  /**
   * @brief Check if partition number is valid
   * @param partition_num Partition number
   * @return Boolean, true if valid
   */
  bool is_valid(std::size_t partition_num) const {
    if(partition_num < blocks_.size())
      return true;
    else return false;
  }

  /**
   * @brief Handle command in redirect case
   * @param cmd_id Command identifier
   * @param response Response to be collected
   */

  void handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) override;

  /**
   * @brief Handle multiple commands in redirect case
   * @param cmd_id Command identifier
   * @param args Command arguments
   * @param responses Responses to be collected
   */

  void handle_redirects(int32_t cmd_id,
                        const std::vector<std::string> &args,
                        std::vector<std::string> &responses) override;

  /* Read partition number */
  std::size_t read_partition_;
  /* Read offset number in a partition */
  std::size_t read_offset_;
  /* Write partition number */
  std::size_t write_partition_;
  /* Replica chain clients, each partition only save a replica chain client */
  std::vector<std::shared_ptr<replica_chain_client>> blocks_;
};

}
}

#endif //JIFFY_FILE_CLIENT_H