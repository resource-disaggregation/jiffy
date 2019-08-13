#ifndef JIFFY_LEASE_RENEWAL_WORKER_H
#define JIFFY_LEASE_RENEWAL_WORKER_H

#include <atomic>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
#include <algorithm>
#include "lease_client.h"

namespace jiffy {
namespace directory {
/* Lease renewal worker */
class lease_renewal_worker {
 public:

  /**
   * @brief Constructor
   * @param host Lease server hostname
   * @param port Port number
   */

  lease_renewal_worker(const std::string &host, int port);

  /**
   * @brief Destructor
   */

  ~lease_renewal_worker();

  /**
   * @brief Start lease renewal worker thread
   * Renew leases in the list and sleep until next work period
   */

  void start();

  /**
   * @brief Stop renewal worker
   */

  void stop();

  /**
   * @brief Add file path to to renew list if file path doesn't exist in the list
   * @param path File path
   */

  void add_path(const std::string &path);

  /**
   * @brief Remove path from to renew list
   * @param path File path
   */

  void remove_path(const std::string &path);

  /**
   * @brief Check if path is already in to renew list
   * @param path File path
   * @return Bool value, true if path already in to renew list
   */

  bool has_path(const std::string &path);

 private:
  /* Metadata mutex */
  mutable std::mutex metadata_mtx_;
  /* Worker thread */
  std::thread worker_;
  /* Stop bool */
  std::atomic_bool stop_;
  /* Lease client */
  lease_client ls_;
  /* To renew files */
  std::vector<std::string> to_renew_;
};

}
}

#endif //JIFFY_LEASE_RENEWAL_WORKER_H
