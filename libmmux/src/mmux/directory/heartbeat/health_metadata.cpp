#include "health_metadata.h"

namespace mmux {
namespace directory {

const size_t health_metadata::DEFAULT_HEARTBEAT_PERIOD = 5;

health_metadata::health_metadata()
    : endpoint_(""), last_hb_local_timestamp_(0), last_hb_timestamp_(0), health_(1.0) {}

health_metadata::health_metadata(const std::string &endpoint, std::size_t heartbeat_period)
    : endpoint_(endpoint),
      last_hb_local_timestamp_(0),
      last_hb_timestamp_(0),
      heartbeat_period_(heartbeat_period),
      health_(1.0) {}

void health_metadata::new_heartbeat(std::time_t hb_timestamp, std::time_t local_timestamp) {
  std::unique_lock<std::shared_mutex> lock(mtx_);
  last_hb_timestamp_ = hb_timestamp;
  last_hb_local_timestamp_ = local_timestamp;
}

void health_metadata::update_health(std::time_t current_local_timestamp) {
  std::shared_lock<std::shared_mutex> lock(mtx_);
  health_ = ((double) heartbeat_period_) / (double) (current_local_timestamp - last_hb_local_timestamp_);
}

double health_metadata::health() const {
  return health_;
}

}
}
