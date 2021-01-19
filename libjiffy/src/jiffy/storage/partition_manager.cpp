#include "partition_manager.h"
#include "jiffy/utils/logger.h"

namespace jiffy {
namespace storage {

using namespace jiffy::utils;

void partition_manager::register_impl(const std::string &type,
                                      std::shared_ptr<partition_builder> impl) {
  implementations().emplace(type, impl);
  LOG(log_level::info) << "Registered implementation for " << type;
}

std::shared_ptr<chain_module> partition_manager::build_partition(block_memory_manager *manager,
                                                                 const std::string &type,
                                                                 const std::string &name,
                                                                 const std::string &metadata,
                                                                 const utils::property_map &conf,
                                                                 const std::string &auto_scaling_host,
                                                                 const int auto_scaling_port) {
  auto it = implementations().find(type);
  if (it == implementations().end()) {
    return nullptr;
  }
  return it->second->build(manager,
                           name,
                           metadata,
                           conf,
                           auto_scaling_host,
                           auto_scaling_port);
}

partition_manager::partition_map &partition_manager::implementations() {
  static partition_map *map = new partition_map();
  return *map;
}

}
}