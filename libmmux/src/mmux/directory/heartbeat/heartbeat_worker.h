#ifndef MEMORYMUX_HEARTBEAT_WORKER_H
#define MEMORYMUX_HEARTBEAT_WORKER_H

#include <cstdint>
#include "../fs/directory_tree.h"
#include "health_metadata.h"

namespace mmux {
namespace directory {

class heartbeat_worker {
 public:
  heartbeat_worker(std::shared_ptr<directory_management_ops> mgmt,
                   std::shared_ptr<std::map<std::string, health_metadata>> hm_map,
                   uint64_t heartbeat_period_ms);
  ~heartbeat_worker();

  void start();

  void stop();
 private:
  void update_health();

  std::shared_ptr<directory_management_ops> mgmt_;
  std::shared_ptr<std::map<std::string, health_metadata>> hm_map_;
  std::chrono::milliseconds heartbeat_period_;

  std::thread worker_;
  std::atomic_bool stop_;
};

}
}

#endif //MEMORYMUX_HEARTBEAT_WORKER_H
