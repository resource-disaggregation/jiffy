#include "heartbeat_service_handler.h"
#include "../../utils/logger.h"

namespace mmux {
namespace directory {

using namespace utils;

heartbeat_service_handler::heartbeat_service_handler(std::shared_ptr<block_allocator> alloc,
                                                     std::shared_ptr<directory_management_ops> mgmt,
                                                     std::shared_ptr<std::map<std::string, health_metadata>> hm_map)
    : alloc_(alloc), mgmt_(mgmt), hm_map_(hm_map) {}

void heartbeat_service_handler::ping(const heartbeat &hb) {
  LOG(log_level::debug) << "Received heartbeat from handler " << hb.sender;
  hm_map_->at(hb.sender).new_heartbeat(hb.timestamp, std::time(nullptr));
}

}
}
