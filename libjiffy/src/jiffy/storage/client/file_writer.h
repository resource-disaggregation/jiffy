#ifndef JIFFY_FILE_WRITER_H
#define JIFFY_FILE_WRITER_H

#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/client_cache.h"
#include "jiffy/storage/file/file_ops.h"
#include "jiffy/storage/client/file_client.h"

namespace jiffy {
namespace storage {

class file_writer : public file_client {
 public:

  /**
   * @brief Constructor
   * Store all replica chain and their begin slot
   * @param fs Directory service
   * @param path Key value block path
   * @param status Data status
   * @param timeout_ms Timeout
   */
  file_writer(std::shared_ptr<directory::directory_interface> fs,
              const std::string &path,
              const directory::data_status &status,
              int timeout_ms = 1000);

  /**
   * @brief Destructor
   */
  virtual ~file_writer() = default;


  /**
   * @brief Write message to file
   * @param data New message
   * @return Response of the command
   */

  std::string write(const std::string &data);

 private:

  /**
   * @brief Handle command in redirect case
   * @param cmd_id Command identifier
   * @param response Response to be collected
   */
  void handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) override;

};

}
}

#endif //JIFFY_FILE_WRITER_H
