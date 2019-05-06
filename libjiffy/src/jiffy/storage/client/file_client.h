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
   * @return Response of the command
   */

  std::string read();

  /**
   * @brief Write message in batch
   * @param msgs New messages
   * @return Response of the commands
   */

  //std::vector<std::string> write(const std::vector<std::string> &msgs);

  /**
   * @brief Read message in batch
   * @param num_msg Number of message to be read in batch
   * @return Response of batch command
   */

  //std::vector<std::string> read(std::size_t num_msg);

 private:

  /**
   * @brief Fetch block identifier for specified operation
   * @param op Operation
   * @return Block identifier
   */

  std::size_t block_id(const file_cmd_id &op);

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

  /* Read start */
  std::size_t read_partition_;
  std::size_t read_offset_;
  std::size_t write_partition_;
};

}
}

#endif //JIFFY_FILE_CLIENT_H