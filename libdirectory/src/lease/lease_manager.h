#ifndef ELASTICMEM_LEASE_MANAGER_H
#define ELASTICMEM_LEASE_MANAGER_H

#include <thread>
#include "../tree/directory_tree.h"

namespace elasticmem {
namespace directory {

class lease_manager {
 public:
  lease_manager(std::shared_ptr<directory_tree> tree, std::uint64_t lease_period_ms, std::uint64_t grace_period_ms);
  ~lease_manager();

  void start();

  void stop();

 private:
  void remove_expired_leases();

  void remove_expired_nodes(std::shared_ptr<ds_dir_node> parent, const std::string &child_name, std::uint64_t epoch);

  std::chrono::milliseconds lease_period_ms_;
  std::chrono::milliseconds grace_period_ms_;
  std::shared_ptr<directory_tree> tree_;
  std::thread worker_;
  std::atomic_bool stop_;
};

}
}

#endif //ELASTICMEM_LEASE_MANAGER_H
