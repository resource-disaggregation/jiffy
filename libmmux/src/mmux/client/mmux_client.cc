#include "mmux_client.h"

namespace mmux {
namespace client {

using namespace directory;

mmux_client::mmux_client(const std::string &host, int dir_port, int lease_port)
    : fs_(std::make_shared<directory_client>(host, dir_port)),
      lease_worker_(host, lease_port) {
  lease_worker_.start();
}

std::shared_ptr<directory::directory_client> mmux_client::fs() {
  return fs_;
}

directory::lease_renewal_worker &mmux_client::lease_worker() {
  return lease_worker_;
}

void mmux_client::begin_scope(const std::string &path) {
  lease_worker_.add_path(path);
}

void mmux_client::end_scope(const std::string &path) {
  lease_worker_.remove_path(path);
}

std::shared_ptr<storage::kv_client> mmux_client::create(const std::string &path,
                                                        const std::string &backing_path,
                                                        size_t num_blocks,
                                                        size_t chain_length,
                                                        int32_t flags) {
  auto s = fs_->create(path, backing_path, num_blocks, chain_length, flags);
  begin_scope(path);
  return std::make_shared<storage::kv_client>(fs_, path, s);
}

std::shared_ptr<storage::kv_client> mmux_client::open(const std::string &path) {
  auto s = fs_->open(path);
  begin_scope(path);
  return std::make_shared<storage::kv_client>(fs_, path, s);
}

std::shared_ptr<storage::kv_client> mmux_client::open_or_create(const std::string &path,
                                                                const std::string &backing_path,
                                                                size_t num_blocks,
                                                                size_t chain_length,
                                                                int32_t flags) {
  auto s = fs_->open_or_create(path, backing_path, num_blocks, chain_length, flags);
  begin_scope(path);
  return std::make_shared<storage::kv_client>(fs_, path, s);
}

std::shared_ptr<storage::kv_listener> mmux_client::listen(const std::string &path) {
  auto s = fs_->open(path);
  begin_scope(path);
  return std::make_shared<storage::kv_listener>(path, s);
}

void mmux_client::remove(const std::string &path) {
  end_scope(path);
  fs_->remove(path);
}

void mmux_client::sync(const std::string &path, const std::string &dest) {
  fs_->sync(path, dest);
}

void mmux_client::dump(const std::string &path, const std::string &dest) {
  fs_->dump(path, dest);
}

void mmux_client::load(const std::string &path, const std::string &dest) {
  fs_->load(path, dest);
}

void mmux_client::close(const std::string &path) {
  end_scope(path);
}

}
}
