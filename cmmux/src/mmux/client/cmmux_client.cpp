#include <mmux/client/mmux_client.h>
#include "cmmux_client.h"

mmux_client *create_mmux_client(const char *host, int32_t dir_port, int32_t lease_port) {
  mmux::client::mmux_client *c = new mmux::client::mmux_client(std::string(host), dir_port, lease_port);
  return c;
}

void destroy_mmux_client(mmux_client *client) {
  mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
  delete c;
}

void mmux_begin_scope(mmux_client *client, const char *path) {
  mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
  c->begin_scope(std::string(path));
}

void mmux_end_scope(mmux_client *client, const char *path) {
  mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
  c->end_scope(std::string(path));
}

directory_client *mmux_get_fs(mmux_client *client) {
  mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
  return reinterpret_cast<directory_client *>(c->fs().get());
}

kv_client *mmux_create(mmux_client *client,
                       const char *path,
                       const char *backing_path,
                       size_t num_blocks,
                       size_t chain_length,
                       int32_t flags) {
  mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
  std::string path_str(path);
  auto s = c->fs()->create(path_str, std::string(backing_path), num_blocks, chain_length, flags);
  c->begin_scope(path_str);
  return new mmux::storage::kv_client(c->fs(), path_str, s);
}

kv_client *mmux_open(mmux_client *client, const char *path) {
  mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
  std::string path_str(path);
  auto s = c->fs()->open(path_str);
  c->begin_scope(path_str);
  return new mmux::storage::kv_client(c->fs(), path_str, s);
}

kv_client *mmux_open_or_create(mmux_client *client,
                               const char *path,
                               const char *backing_path,
                               size_t num_blocks,
                               size_t chain_length,
                               int32_t flags) {
  mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
  std::string path_str(path);
  auto s = c->fs()->open_or_create(path_str, std::string(backing_path), num_blocks, chain_length, flags);
  c->begin_scope(path_str);
  return new mmux::storage::kv_client(c->fs(), path_str, s);
}

kv_listener *mmux_listen(mmux_client *client, const char *path) {
  mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
  std::string path_str(path);
  auto s = c->fs()->open(path_str);
  c->begin_scope(path_str);
  return new mmux::storage::kv_listener(path_str, s);
}

void mmux_close(mmux_client *client, const char *path) {
  mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
  c->close(std::string(path));
}

void mmux_remove(mmux_client *client, const char *path) {
  mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
  c->remove(std::string(path));
}

void mmux_sync(mmux_client *client, const char *path, const char *backing_path) {
  mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
  c->sync(std::string(path), std::string(backing_path));
}

void mmux_dump(mmux_client *client, const char *path, const char *backing_path) {
  mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
  c->dump(std::string(path), std::string(backing_path));
}

void mmux_load(mmux_client *client, const char *path, const char *backing_path) {
  mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
  c->load(std::string(path), std::string(backing_path));
}
