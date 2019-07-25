#ifndef JIFFY_FILE_READER_H
#define JIFFY_FILE_READER_H

#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/client_cache.h"
#include "jiffy/storage/file/file_ops.h"
#include "jiffy/storage/client/file_client.h"

namespace jiffy {
namespace storage {

class file_reader: public file_client {
 public:
  /**
   * @brief Constructor
   * Store all replica chain and their begin slot
   * @param fs Directory service
   * @param path Key value block path
   * @param status Data status
   * @param timeout_ms Timeout
   */
  file_reader(std::shared_ptr<directory::directory_interface> fs,
              const std::string &path,
              const directory::data_status &status,
              int timeout_ms = 1000);

  /**
   * @brief Destructor
   */
  ~file_reader() override = default;

  /**
   * @brief Read message from file
   * @param size Size to be read
   * @return Response of the command
   */
  std::string read(std::size_t size);

 private:

  /**
   * @brief Handle command in redirect case
   * @param _return Response to be collected
   */
  void handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) override;
};

}
}

#endif //JIFFY_FILE_READER_H
