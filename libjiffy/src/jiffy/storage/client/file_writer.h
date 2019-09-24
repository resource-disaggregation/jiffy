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
  ~file_writer() override = default;


  /**
   * @brief Write message to file
   * @param data New message
   * @return Response of the command
   */

  void write(const std::string &data);

 private:

  /**
   * @brief Handle command in redirect case
   * @param _return Response to be collected
   */
  void handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) override;

};

}
}

#endif //JIFFY_FILE_WRITER_H
