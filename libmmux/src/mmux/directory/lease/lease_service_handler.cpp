#include "lease_service_handler.h"
#include "../../utils/logger.h"

#include <utility>

namespace mmux {
namespace directory {

using namespace utils;

/**
 * @brief Construction function
 * @param tree directory tree
 * @param lease_period_ms lease duration
 */

lease_service_handler::lease_service_handler(std::shared_ptr<directory_tree> tree, int64_t lease_period_ms)
    : tree_(std::move(tree)), lease_period_ms_(lease_period_ms) {}

/**
 * @brief Renew leases
 * @param _return rpc lease acknowledgement
 * @param to_renew vector of paths to be renewed
 */

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

/*
 * @brief Make exception
 */

lease_service_exception lease_service_handler::make_exception(const directory_ops_exception &ex) {
  lease_service_exception e;
  e.msg = ex.what();
  return e;
}

}
}
