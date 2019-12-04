#include <jiffy/utils/string_utils.h>
#include "default_partition.h"
#include "jiffy/storage/client/replica_chain_client.h"
#include "jiffy/utils/logger.h"
#include "jiffy/persistent/persistent_store.h"
#include "jiffy/storage/partition_manager.h"
#include "jiffy/directory/client/directory_client.h"
#include "jiffy/auto_scaling/auto_scaling_client.h"
#include <thread>

namespace jiffy {
namespace storage {

using namespace utils;

default_partition::default_partition(block_memory_manager *manager,
                                     block_response_client_map &response_map,
                                     const std::string &name,
                                     const std::string &metadata,
                                     const utils::property_map &,
                                     const std::string &,
                                     int)
    : chain_module(manager, response_map, name, metadata, {}) {}

void default_partition::run_command(std::vector<std::string> &_return,
                                    const std::vector<std::string> &) {
  LOG(log_level::info) << "Asking the default partition";
  _return.emplace_back("!block_moved");
}

void default_partition::load(const std::string &) {}

bool default_partition::sync(const std::string &) {
  return false;
}

bool default_partition::dump(const std::string &) {
  return false;
}

void default_partition::forward_all() {}

REGISTER_IMPLEMENTATION("default", default_partition);

}
}