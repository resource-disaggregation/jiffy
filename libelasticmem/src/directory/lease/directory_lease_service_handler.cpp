#include "directory_lease_service_handler.h"
#include "../../utils/logger.h"

#include <utility>

namespace elasticmem {
namespace directory {

using namespace utils;

directory_lease_service_handler::directory_lease_service_handler(std::shared_ptr<directory_tree> tree)
    : tree_(std::move(tree)) {}

void directory_lease_service_handler::update_leases(rpc_lease_ack &_return, const rpc_lease_update &updates) {
  try {
    // Handle renewals
    for (const auto &path: updates.to_renew) {
      tree_->touch(path);
      LOG(log_level::trace) << "Renewed lease for " << path;
      ++_return.renewed;
    }

    // Handle flushes
    _return.flushed = 0;
    for (const auto &path: updates.to_flush) {
      tree_->flush(path);
      LOG(log_level::trace) << "Flushed " << path;
      ++_return.flushed;
    }

    // Handle removals
    _return.removed = 0;
    for (const auto &path: updates.to_remove) {
      tree_->remove_all(path);
      LOG(log_level::trace) << "Removed " << path;
      ++_return.removed;
    }
  } catch (directory_ops_exception &ex) {
    throw make_exception(ex);
  }
}

directory_lease_service_exception directory_lease_service_handler::make_exception(const directory_ops_exception &ex) {
  directory_lease_service_exception e;
  e.msg = ex.what();
  return e;
}

}
}
