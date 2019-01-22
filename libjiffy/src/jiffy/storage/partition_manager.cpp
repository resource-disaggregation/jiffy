#include "partition_manager.h"

namespace jiffy {
namespace storage {

void partition_manager::register_impl(const std::string &type,
                                      std::shared_ptr<partition_builder> impl) {
  implementations().emplace(type, impl);
}

std::shared_ptr<chain_module> partition_manager::build_partition(const std::string &type,
                                                                 const std::string &name,
                                                                 const std::string &metadata,
                                                                 const utils::property_map &conf) {
  auto it = implementations().find(type);
  if (it == implementations().end()) {
    return nullptr;
  }
  return it->second->build(name, metadata, conf);
}

partition_manager::partition_map &partition_manager::implementations() {
  static partition_map *map = new partition_map();
  return *map;
}

}
}