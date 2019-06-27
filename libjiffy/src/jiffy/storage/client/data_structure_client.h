#ifndef JIFFY_DATA_STRUCTURE_CLIENT_H
#define JIFFY_DATA_STRUCTURE_CLIENT_H

#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/client_cache.h"

namespace jiffy {
namespace storage {

/* Redo when exception class
 * Redo whenever exception happens */
class redo_error : public std::exception {
 public:
  redo_error() = default;
};

/* Data structure client */
class data_structure_client {
 public:
  /**
   * @brief Constructor
   * @param fs Directory service
   * @param path Key value block path
   * @param OPS operation type
   * @param status Data status
   * @param timeout_ms Timeout
   */

  data_structure_client(std::shared_ptr<directory::directory_interface> fs,
                        const std::string &path,
                        const directory::data_status &status,
                        std::vector<command> OPS,
                        int timeout_ms = 1000);

  /**
   * @brief Refresh the slot and blocks from directory service
   */

  virtual void refresh() = 0;

  /**
   * @brief Fetch data status
   * @return Data status
   */

  directory::data_status &status();

 protected:

  /**
   * @brief Handle command in redirect case
   * @param cmd_id Command identifier
   * @param args Command arguments
   * @param response Response to be collected
   */

  virtual void handle_redirect(int32_t cmd_id, const std::vector<std::string> &args, std::string &response) = 0;

  /**
   * @brief Handle multiple commands in redirect case
   * @param cmd_id Command identifier
   * @param args Command arguments
   * @param responses Responses to be collected
   */

  virtual void handle_redirects(int32_t cmd_id,
                                std::vector<std::string> &args,
                                std::vector<std::string> &responses) = 0;

  /* Directory client */
  std::shared_ptr<directory::directory_interface> fs_;
  /* Key value partition path */
  std::string path_;
  /* Data status */
  directory::data_status status_;

  /* Time out*/
  int timeout_ms_;
};

}
}

#endif //JIFFY_DATA_STRUCTURE_CLIENT_H
