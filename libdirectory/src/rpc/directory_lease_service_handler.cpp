#include "directory_lease_service_handler.h"

#include <utility>

namespace elasticmem {
namespace directory {

directory_lease_service_handler::directory_lease_service_handler(std::shared_ptr<directory_tree> tree,
                                                                 std::shared_ptr<kv::kv_management_service> kv)
    : tree_(std::move(tree)), kv_(std::move(kv)) {}

void directory_lease_service_handler::create(const std::string &path, const std::string &persistent_store_prefix) {
  try {
    tree_->create_file(path);
    tree_->persistent_store_prefix(path, persistent_store_prefix);
  } catch (directory_service_exception &ex) {
    throw make_exception(ex);
  }
}

void directory_lease_service_handler::load(const std::string &path) {
  try {
    auto s = tree_->dstatus(path);
    for (const std::string &block_name: s.data_blocks()) {
      kv_->load(block_name, s.persistent_store_prefix(), path);
    }
  } catch (directory_service_exception &ex) {
    throw make_exception(ex);
  }
}

void directory_lease_service_handler::renew_lease(rpc_keep_alive_ack &_return,
                                                  const std::string &path,
                                                  const int64_t bytes_added) {
  try {
    tree_->touch(path);
    if (tree_->is_regular_file(path)) {
      if (bytes_added < 0) {
        tree_->shrink(path, static_cast<size_t>(std::abs(bytes_added)));
      } else {
        tree_->grow(path, static_cast<size_t>(std::abs(bytes_added)));
      }
    }
    _return.path = path;
    _return.tot_bytes = static_cast<int64_t>(tree_->file_size(path));
  } catch (directory_service_exception &ex) {
    throw make_exception(ex);
  }
}

void directory_lease_service_handler::remove(const std::string &path, const rpc_remove_mode mode) {
  auto s = tree_->dstatus(path);
  for (const std::string &block_name: s.data_blocks()) {
    if (mode == rpc_remove_mode::rpc_flush) {
      kv_->flush(block_name, s.persistent_store_prefix(), path);
    } else {
      kv_->clear(block_name);
    }
  }
}

directory_lease_service_exception directory_lease_service_handler::make_exception(const directory_service_exception &ex) {
  directory_lease_service_exception e;
  e.msg = ex.what();
  return e;
}

}
}
