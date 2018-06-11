#include "mmux_client.h"

namespace mmux {
namespace client {

using namespace directory;

mmux_client::mmux_client(const std::string &host, int dir_port, int lease_port)
    : fs_(std::make_shared<directory_client>(host, dir_port)),
      lease_worker_(host, lease_port) {
}

void mmux_client::begin_scope(const std::string &path) {
  lease_worker_.add_path(path);
}

void mmux_client::end_scope(const std::string &path) {
  lease_worker_.remove_path(path);
}

storage::kv_client mmux_client::create(const std::string &path,
                                       const std::string &persistent_store_prefix,
                                       size_t num_blocks,
                                       size_t chain_length) {
  auto s = fs_->create(path, persistent_store_prefix, num_blocks, chain_length);
  begin_scope(path);
  return storage::kv_client(fs_, path, s);
}

storage::kv_client mmux_client::open(const std::string &path) {
  auto s = fs_->open(path);
  begin_scope(path);
  return storage::kv_client(fs_, path, s);
}

storage::kv_client mmux_client::open_or_create(const std::string &path,
                                               const std::string &persistent_store_prefix,
                                               size_t num_blocks,
                                               size_t chain_length) {
  auto s = fs_->open_or_create(path, persistent_store_prefix, num_blocks, chain_length);
  begin_scope(path);
  return storage::kv_client(fs_, path, s);
}

void mmux_client::remove(const std::string &path) {
  end_scope(path);
  fs_->remove(path);
}

void mmux_client::flush(const std::string &path) {
  end_scope(path);
  fs_->flush(path);
}

}
}
