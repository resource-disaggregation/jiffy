#include <mmux/client/mmux_client.h>
#include "cmmux_client.h"

mmux_client *create_mmux_client(const char *host, int32_t dir_port, int32_t lease_port) {
  try {
    mmux::client::mmux_client *c = new mmux::client::mmux_client(std::string(host), dir_port, lease_port);
    return c;
  } catch (std::exception &e) {
    return NULL;
  }
}

int destroy_mmux_client(mmux_client *client) {
  try {
    mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
    delete c;
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int mmux_begin_scope(mmux_client *client, const char *path) {
  try {
    mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
    c->begin_scope(std::string(path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int mmux_end_scope(mmux_client *client, const char *path) {
  try {
    mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
    c->end_scope(std::string(path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

directory_client *mmux_get_fs(mmux_client *client) {
  try {
    mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
    return reinterpret_cast<directory_client *>(c->fs().get());
  } catch (std::exception &e) {
    return NULL;
  }
}

kv_client *mmux_create(mmux_client *client,
                       const char *path,
                       const char *backing_path,
                       size_t num_blocks,
                       size_t chain_length,
                       int32_t flags) {
  try {
    mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
    std::string path_str(path);
    auto s = c->fs()->create(path_str, std::string(backing_path), num_blocks, chain_length, flags);
    c->begin_scope(path_str);
    return new mmux::storage::kv_client(c->fs(), path_str, s);
  } catch (std::exception &e) {
    return NULL;
  }
}

kv_client *mmux_open(mmux_client *client, const char *path) {
  try {
    mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
    std::string path_str(path);
    auto s = c->fs()->open(path_str);
    c->begin_scope(path_str);
    return new mmux::storage::kv_client(c->fs(), path_str, s);
  } catch (std::exception &e) {
    return NULL;
  }
}

kv_client *mmux_open_or_create(mmux_client *client,
                               const char *path,
                               const char *backing_path,
                               size_t num_blocks,
                               size_t chain_length,
                               int32_t flags) {
  try {
    mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
    std::string path_str(path);
    auto s = c->fs()->open_or_create(path_str, std::string(backing_path), num_blocks, chain_length, flags);
    c->begin_scope(path_str);
    return new mmux::storage::kv_client(c->fs(), path_str, s);
  } catch (std::exception &e) {
    return NULL;
  }
}

kv_listener *mmux_listen(mmux_client *client, const char *path) {
  try {
    mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
    std::string path_str(path);
    auto s = c->fs()->open(path_str);
    c->begin_scope(path_str);
    return new mmux::storage::kv_listener(path_str, s);
  } catch (std::exception &e) {
    return NULL;
  }
}

int mmux_close(mmux_client *client, const char *path) {
  try {
    mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
    c->close(std::string(path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int mmux_remove(mmux_client *client, const char *path) {
  try {
    mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
    c->remove(std::string(path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int mmux_sync(mmux_client *client, const char *path, const char *backing_path) {
  try {
    mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
    c->sync(std::string(path), std::string(backing_path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int mmux_dump(mmux_client *client, const char *path, const char *backing_path) {
  try {
    mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
    c->dump(std::string(path), std::string(backing_path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int mmux_load(mmux_client *client, const char *path, const char *backing_path) {
  try {
    mmux::client::mmux_client *c = static_cast<mmux::client::mmux_client *>(client);
    c->load(std::string(path), std::string(backing_path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}
