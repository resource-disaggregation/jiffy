#ifndef JIFFY_LEASE_MANAGER_H
#define JIFFY_LEASE_MANAGER_H

#include <thread>
#include "../fs/directory_tree.h"

namespace jiffy {
namespace directory {
/* Lease_expiry worker class */
class lease_expiry_worker {
 public:

  /**
   * @brief Constructor
   * @param tree Directory tree
   * @param lease_period_ms Worker time period
   * @param grace_period_ms Extended time
   */

  lease_expiry_worker(std::shared_ptr<directory_tree> tree, uint64_t lease_period_ms, uint64_t grace_period_ms);

  /**
   * @brief Destructor
  */

  ~lease_expiry_worker();

  /**
   * @brief Start worker
   * Remove expired leases and sleep until next time period
   */

  void start();

  /**
   * @brief Stop worker
   */

  void stop();

  /**
   * @brief Fetch epochs
   * @return Time epochs
   */

  size_t num_epochs() const;

 private:

  /**
   * @brief Remove lease expired starting from root
   */

  void remove_expired_leases();

  /**
   * @brief Recursively remove expired children
   * @param parent Parent node
   * @param parent_path Child's path to root
   * @param child_name Child name
   * @param epoch Time epoch
   */

  void remove_expired_nodes(std::shared_ptr<ds_dir_node> parent,
                            const std::string &parent_path,
                            const std::string &child_name,
                            std::uint64_t epoch);
  /* Lease duration */
  std::chrono::milliseconds lease_period_ms_;
  /* Extended lease duration */
  std::chrono::milliseconds grace_period_ms_;
  /* Directory tree */
  std::shared_ptr<directory_tree> tree_;
  /* Worker thread */
  std::thread worker_;
  /* Stop bool */
  std::atomic_bool stop_;
  /* number epochs */
  std::atomic_size_t num_epochs_;
};

}
}

#endif //JIFFY_LEASE_MANAGER_H
