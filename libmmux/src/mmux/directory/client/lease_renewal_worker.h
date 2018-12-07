#ifndef MMUX_LEASE_RENEWAL_WORKER_H
#define MMUX_LEASE_RENEWAL_WORKER_H

#include <atomic>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "lease_client.h"

namespace mmux {
namespace directory {
/* Lease renewal worker */
class lease_renewal_worker {
 public:

  /**
   * @brief Constructor
   * @param host Socket host
   * @param port Socket port
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
   * @brief Add file path to to_renew list if file path doesn't already exist in the list
   * @param path File path
   */

  void add_path(const std::string &path);

  /**
   * @brief Remove path from to_renew list
   * @param path File path
   */

  void remove_path(const std::string &path);

  /**
   * @brief Check if path is already in to_renew list
   * @param path File path
   * @return Bool value
   */

  bool has_path(const std::string &path);

 private:
  /* Metadata mutex */
  mutable std::shared_mutex metadata_mtx_;
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

#endif //MMUX_LEASE_RENEWAL_WORKER_H
