#ifndef MMUX_SYNC_WORKER_H
#define MMUX_SYNC_WORKER_H

#include <chrono>
#include "directory_tree.h"

namespace mmux {
namespace directory {

class sync_worker {
 public:
  sync_worker(std::shared_ptr<directory_tree> tree, uint64_t sync_period_ms);
  ~sync_worker();

  void start();

  void stop();
 private:
  void sync_nodes();

  void sync_nodes(std::shared_ptr<ds_dir_node> parent,
                  const std::string &parent_path,
                  const std::string &child_name,
                  std::uint64_t epoch);

  std::shared_ptr<directory_tree> tree_;
  std::chrono::milliseconds sync_period_;

  std::thread worker_;
  std::atomic_bool stop_;
};

}
}

#endif //MMUX_SYNC_WORKER_H
