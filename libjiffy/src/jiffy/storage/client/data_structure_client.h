#ifndef JIFFY_DATA_STRUCTURE_CLIENT_H
#define JIFFY_DATA_STRUCTURE_CLIENT_H

#include "jiffy/directory/client/directory_client.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/client_cache.h"

#define THROW_IF_NOT_OK(ret) if (ret[0] != "!ok") throw std::logic_error(ret[0])

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
   * @param status Data status
   * @param timeout_ms Timeout
   */

  data_structure_client(std::shared_ptr<directory::directory_interface> fs,
                        const std::string &path,
                        const directory::data_status &status,
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
   * @param args Command arguments
   * @param _return Response to be collected
   */

  virtual void handle_redirect(std::vector<std::string> &_return, const std::vector<std::string> &args) = 0;

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
