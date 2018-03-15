#include "directory_lease_service_handler.h"

#include <utility>

namespace elasticmem {
namespace directory {

directory_lease_service_handler::directory_lease_service_handler(std::shared_ptr<directory_tree> tree,
                                                                 std::shared_ptr<storage::storage_management_service> storage)
    : tree_(std::move(tree)), storage_(std::move(storage)) {}

void directory_lease_service_handler::update_leases(rpc_lease_ack &_return, const rpc_lease_update &updates) {
  try {
    // Handle renewals
    for (const auto &path: updates.to_renew) {
      tree_->touch(path);
      ++_return.renewed;
    }

    // Handle flushes
    _return.flushed = 0;
    for (const auto &path: updates.to_flush) {
      tree_->mode(path, storage_mode::flushing);
      auto s = tree_->dstatus(path);
      for (const std::string &block_name: s.data_blocks()) {
        storage_->flush(block_name, s.persistent_store_prefix(), path);
      }
      tree_->mode(path, storage_mode::on_disk);
      ++_return.flushed;
    }

    // Handle removals
    _return.removed = 0;
    for (const auto &path: updates.to_remove) {
      auto blocks = tree_->data_blocks(path);
      tree_->remove_all(path);
      for (const std::string &block_name: blocks) {
        storage_->clear(block_name);
      }
      ++_return.removed;
    }
  } catch (directory_service_exception &ex) {
    throw make_exception(ex);
  }
}

directory_lease_service_exception directory_lease_service_handler::make_exception(const directory_service_exception &ex) {
  directory_lease_service_exception e;
  e.msg = ex.what();
  return e;
}

}
}
