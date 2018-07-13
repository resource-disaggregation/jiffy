#ifndef MEMORYMUX_HEARTBEAT_METADATA_H
#define MEMORYMUX_HEARTBEAT_METADATA_H

#include <ctime>
#include <string>
#include <shared_mutex>

namespace mmux {
namespace directory {

class health_metadata {
 public:
  static const size_t DEFAULT_HEARTBEAT_PERIOD;
  health_metadata();
  health_metadata(const std::string& endpoint, std::size_t heartbeat_period);

  void new_heartbeat(std::time_t hb_timestamp, std::time_t local_timestamp);

  void update_health(std::time_t current_local_timestamp);

  double health() const;

 private:
  std::shared_mutex mtx_;
  std::string endpoint_;
  std::time_t last_hb_local_timestamp_;
  std::time_t last_hb_timestamp_;
  std::size_t heartbeat_period_;
  double health_;
};
}
}

#endif //MEMORYMUX_HEARTBEAT_METADATA_H
