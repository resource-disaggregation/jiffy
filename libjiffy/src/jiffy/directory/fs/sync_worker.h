#ifndef JIFFY_SYNC_WORKER_H
#define JIFFY_SYNC_WORKER_H

#include <chrono>
#include "directory_tree.h"

namespace jiffy {
namespace directory {
/* Synchronization worker class */
class sync_worker {
 public:
  /**
   * @brief Constructor
   * @param tree Directory tree
   * @param sync_period_ms Synchronization worker working period
   */

  sync_worker(std::shared_ptr<directory_tree> tree, uint64_t sync_period_ms);

  /**
   * @brief Destructor
   */

  ~sync_worker();

  /**
   * @brief Start synchronization worker
   */

  void start();

  /**
   * @brief Stop synchronization worker
   */

  void stop();

  /**
   * @brief Fetch time epoch of worker
   * @return Number epochs
   */

  size_t num_epochs() const;
 private:

  /**
   * @brief Synchronization starting from root directory node
   */

  void sync_nodes();

  /**
   * @brief Synchronization of nodes recursively
   * @param parent Parent directory node
   * @param parent_path  Parent path
   * @param child_name Child node name
   * @param epoch Time epoch
   */

  void sync_nodes(std::shared_ptr<ds_dir_node> parent,
                  const std::string &parent_path,
                  const std::string &child_name,
                  std::uint64_t epoch);

  /* Directory tree */
  std::shared_ptr<directory_tree> tree_;
  /* Synchronization working period */
  std::chrono::milliseconds sync_period_;
  /* Worker thread */
  std::thread worker_;
  /* Bool for stopping the worker */
  std::atomic_bool stop_;
  /* Time epoch */
  std::atomic_size_t num_epochs_;
};

}
}

#endif //JIFFY_SYNC_WORKER_H
