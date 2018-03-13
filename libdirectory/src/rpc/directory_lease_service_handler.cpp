#include "directory_lease_service_handler.h"

#include <utility>

namespace elasticmem {
namespace directory {

directory_lease_service_handler::directory_lease_service_handler(std::shared_ptr<directory_tree> tree,
                                                                 std::shared_ptr<kv::kv_management_service> kv)
    : tree_(std::move(tree)), kv_(std::move(kv)) {}

void directory_lease_service_handler::update_leases(lease_ack &_return, const lease_update &updates) {
  try {
    // Handle renewals
    for (const auto &entry: updates.to_renew) {
      tree_->touch(entry.path);
      if (tree_->is_regular_file(entry.path)) {
        if (entry.bytes < 0) {
          tree_->shrink(entry.path, static_cast<size_t>(std::abs(entry.bytes)));
        } else {
          tree_->grow(entry.path, static_cast<size_t>(std::abs(entry.bytes)));
        }
      }
      rpc_file_metadata renew_ack;
      renew_ack.path = entry.path;
      renew_ack.bytes = static_cast<int64_t>(tree_->file_size(entry.path));
      _return.renewed.push_back(renew_ack);
    }

    // Handle flushes
    _return.flushed = 0;
    for (const auto &path: updates.to_flush) {
      auto s = tree_->dstatus(path);
      for (const std::string &block_name: s.data_blocks()) {
        kv_->flush(block_name, s.persistent_store_prefix(), path);
      }
      ++_return.flushed;
    }

    // Handle removals
    _return.removed = 0;
    for (const auto &path: updates.to_remove) {
      auto s = tree_->dstatus(path);
      for (const std::string &block_name: s.data_blocks()) {
        kv_->clear(block_name);
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
