#ifndef MMUX_LEASE_MANAGER_H
#define MMUX_LEASE_MANAGER_H

#include <thread>
#include "../fs/directory_tree.h"

namespace mmux {
namespace directory {

class lease_expiry_worker {
 public:
  lease_expiry_worker(std::shared_ptr<directory_tree> tree, uint64_t lease_period_ms, uint64_t grace_period_ms);
  ~lease_expiry_worker();

  void start();

  void stop();

 private:
  void remove_expired_leases();

  void remove_expired_nodes(std::shared_ptr<ds_dir_node> parent,
                            const std::string &parent_path,
                            const std::string &child_name,
                            std::uint64_t epoch);

  std::chrono::milliseconds lease_period_ms_;
  std::chrono::milliseconds grace_period_ms_;
  std::shared_ptr<directory_tree> tree_;
  std::thread worker_;
  std::atomic_bool stop_;
};

}
}

#endif //MMUX_LEASE_MANAGER_H
