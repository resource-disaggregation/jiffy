#include "lease_service_handler.h"
#include "../../utils/logger.h"

#include <utility>

namespace jiffy {
namespace directory {

using namespace utils;

lease_service_handler::lease_service_handler(std::shared_ptr<directory_tree> tree, int64_t lease_period_ms)
    : tree_(std::move(tree)), lease_period_ms_(lease_period_ms) {}

void lease_service_handler::renew_leases(rpc_lease_ack &_return, const std::vector<std::string> &to_renew) {
  try {
    // Handle renewals
    for (const auto &path: to_renew) {
      tree_->touch(path);
      LOG(log_level::trace) << "Renewed lease for " << path;
      ++_return.renewed;
    }
    _return.lease_period_ms = lease_period_ms_;
  } catch (directory_ops_exception &ex) {
    LOG(log_level::error) << "Error renewing lease: " << ex.what();
    throw make_exception(ex);
  }
}

lease_service_exception lease_service_handler::make_exception(const directory_ops_exception &ex) {
  lease_service_exception e;
  e.msg = ex.what();
  return e;
}

}
}
