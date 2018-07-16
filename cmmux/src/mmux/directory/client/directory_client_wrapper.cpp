#include <string.h>
#include "directory_client_wrapper.h"
#include "mmux/directory/client/directory_client.h"

data_status from_cxx(const mmux::directory::data_status &s) {
  data_status ret;
  ret.chain_length = s.chain_length();
  ret.num_blocks = s.data_blocks().size();
  ret.backing_path = strdup(s.backing_path().c_str());
  ret.flags = s.flags();
  ret.data_blocks = (replica_chain *) malloc(sizeof(replica_chain) * ret.num_blocks);
  for (size_t i = 0; i < ret.num_blocks; i++) {
    ret.data_blocks[i].storage_mode = (int32_t) s.data_blocks().at(i).mode;
    ret.data_blocks[i].slot_begin = s.data_blocks().at(i).slot_begin();
    ret.data_blocks[i].slot_end = s.data_blocks().at(i).slot_end();
    ret.data_blocks[i].chain_length = s.data_blocks().at(i).block_names.size();
    ret.data_blocks[i].block_names = (char **) malloc(sizeof(char *) * ret.data_blocks[i].chain_length);
    for (size_t j = 0; j < ret.data_blocks[i].chain_length; j++) {
      ret.data_blocks[i].block_names[j] = strdup(s.data_blocks().at(i).block_names.at(j).c_str());
    }
  }
  return ret;
}

file_status from_cxx(const mmux::directory::file_status &s) {
  file_status ret;
  ret.permissions = s.permissions()();
  ret.last_write_time = s.last_write_time();
  ret.type = (int32_t) s.type();
  return ret;
}

directory_entry *from_cxx(const std::vector<mmux::directory::directory_entry> &entries) {
  if (entries.empty())
    return nullptr;
  directory_entry *ret = (directory_entry *) malloc(sizeof(directory_entry));
  directory_entry *cur = ret;
  for (size_t i = 0; i < entries.size() - 1; i++) {
    cur->name = strdup(entries[i].name().c_str());
    cur->status = from_cxx(entries[i].status());
    cur->next = (directory_entry *) malloc(sizeof(directory_entry));
    cur = cur->next;
  }
  cur->name = strdup(entries.back().name().c_str());
  cur->status = from_cxx(entries.back().status());
  cur->next = nullptr;
  return ret;
}

int destroy_fs(directory_client *client) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    delete c;
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int fs_create_directory(directory_client *client, const char *path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    c->create_directory(std::string(path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int fs_create_directories(directory_client *client, const char *path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    c->create_directories(std::string(path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int fs_open(directory_client *client, const char *path, struct data_status *status) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    if (status)
      *status = from_cxx(c->open(std::string(path)));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int fs_create(directory_client *client,
              const char *path,
              const char *backing_path,
              size_t num_blocks,
              size_t chain_length,
              int32_t flags,
              struct data_status *status) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    if (status)
      *status = from_cxx(c->create(std::string(path), std::string(backing_path), num_blocks, chain_length, flags));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int fs_open_or_create(directory_client *client,
                      const char *path,
                      const char *backing_path,
                      size_t num_blocks,
                      size_t chain_length,
                      int32_t flags,
                      struct data_status *status) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    if (status)
      *status =
          from_cxx(c->open_or_create(std::string(path), std::string(backing_path), num_blocks, chain_length, flags));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int fs_exists(directory_client *client, const char *path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    return c->exists(std::string(path));
  } catch (std::exception &e) {
    return -1;
  }
}

int64_t fs_last_write_time(directory_client *client, const char *path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    return c->last_write_time(std::string(path));
  } catch (std::exception &e) {
    return -1;
  }
}

int fs_get_permissions(directory_client *client, const char *path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    return c->permissions(std::string(path))();
  } catch (std::exception &e) {
    return -1;
  }
}

int fs_set_permissions(directory_client *client, const char *path, uint16_t perms, int32_t opts) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    c->permissions(std::string(path), mmux::directory::perms(perms), static_cast<mmux::directory::perm_options>(opts));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int fs_remove(directory_client *client, const char *path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    c->remove(std::string(path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int fs_remove_all(directory_client *client, const char *path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    c->remove_all(std::string(path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int fs_rename(directory_client *client, const char *old_path, const char *new_path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    c->rename(std::string(old_path), std::string(new_path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int fs_status(directory_client *client, const char *path, struct file_status *status) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    *status = from_cxx(c->status(std::string(path)));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

struct directory_entry *fs_directory_entries(directory_client *client, const char *path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    return from_cxx(c->directory_entries(std::string(path)));
  } catch (std::exception &e) {
    return NULL;
  }
}

struct directory_entry *fs_recursive_directory_entries(directory_client *client, const char *path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    return from_cxx(c->recursive_directory_entries(std::string(path)));
  } catch (std::exception &e) {
    return NULL;
  }
}

int fs_dstatus(directory_client *client, const char *path, struct data_status *status) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    *status = from_cxx(c->dstatus(std::string(path)));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int fs_is_regular_file(directory_client *client, const char *path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    return c->is_regular_file(std::string(path));
  } catch (std::exception &e) {
    return -1;
  }
}

int fs_is_directory(directory_client *client, const char *path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    return c->is_directory(std::string(path));
  } catch (std::exception &e) {
    return -1;
  }
}

int fs_sync(directory_client *client, const char *path, const char *backing_path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    c->sync(std::string(path), std::string(backing_path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int fs_dump(directory_client *client, const char *path, const char *backing_path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    c->dump(std::string(path), std::string(backing_path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int fs_load(directory_client *client, const char *path, const char *backing_path) {
  try {
    mmux::directory::directory_client *c = static_cast<mmux::directory::directory_client *>(client);
    c->load(std::string(path), std::string(backing_path));
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

