#include "heartbeat_worker.h"

namespace mmux {
namespace directory {

using namespace utils;

heartbeat_worker::heartbeat_worker(std::shared_ptr<directory_management_ops> mgmt,
                                   std::shared_ptr<std::map<std::string, health_metadata>> hm_map,
                                   uint64_t heartbeat_period_ms)
    : mgmt_(mgmt), hm_map_(hm_map), heartbeat_period_(heartbeat_period_ms) {}

heartbeat_worker::~heartbeat_worker() {
  stop_.store(true);
  if (worker_.joinable())
    worker_.join();
}

void heartbeat_worker::start() {
  worker_ = std::move(std::thread([&] {
    while (!stop_.load()) {
      LOG(log_level::trace) << "Re-computing storage server health...";
      auto start = std::chrono::steady_clock::now();
      try {
        update_health();
      } catch (std::exception &e) {
        LOG(error) << "Exception: " << e.what();
      }
      auto end = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

      auto time_to_wait = std::chrono::duration_cast<std::chrono::milliseconds>(heartbeat_period_ - elapsed);
      if (time_to_wait > std::chrono::milliseconds::zero()) {
        std::this_thread::sleep_for(time_to_wait);
      }
    }
  }));
}

void heartbeat_worker::stop() {
  stop_.store(true);
}

void heartbeat_worker::update_health() {
  for (auto &entry: *hm_map_) {
    entry.second.update_health(std::time(nullptr));
    if (entry.second.health() < 0.3) {
      LOG(log_level::warn) << "Health for " << entry.first << " is low (" << entry.second.health() << ")!";
      // TODO: resolve failures for all files whose blocks are on this endpoint
    } else {
      LOG(log_level::trace) << "Health for " << entry.first << " is ok (" << entry.second.health() << ").";
    }
  }
}

}
}