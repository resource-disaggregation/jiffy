#ifndef MEMORYMUX_DIRECTORY_CLIENT_WRAPPER_H
#define MEMORYMUX_DIRECTORY_CLIENT_WRAPPER_H

#include <stdint.h>
#include <stddef.h>
#include "../../storage/client/kv_client_wrapper.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void directory_client;

struct file_status {
  int32_t type;
  uint16_t permissions;
  int64_t last_write_time;
};

struct directory_entry {
  char *name;
  struct file_status status;
  struct directory_entry *next;
};

struct replica_chain {
  char **block_names;
  size_t chain_length;
  int32_t slot_begin;
  int32_t slot_end;
  int32_t storage_mode;
};

struct data_status {
  char *backing_path;
  size_t chain_length;
  struct replica_chain *data_blocks;
  size_t num_blocks;
  int32_t flags;
};

int destroy_fs(directory_client *client);

int fs_create_directory(directory_client *client, const char *path);
int fs_create_directories(directory_client *client, const char *path);
int fs_open(directory_client *client, const char *path, struct data_status *status);
int fs_create(directory_client *client,
              const char *path,
              const char *backing_path,
              size_t num_blocks,
              size_t chain_length,
              int32_t flags,
              struct data_status *status);
int fs_open_or_create(directory_client *client,
                      const char *path,
                      const char *backing_path,
                      size_t num_blocks,
                      size_t chain_length,
                      int32_t flags,
                      struct data_status *status);
int fs_exists(directory_client *client, const char *path);
int64_t fs_last_write_time(directory_client *client, const char *path);
int fs_get_permissions(directory_client *client, const char *path);
int fs_set_permissions(directory_client *client, const char *path, uint16_t perms, int32_t perm_opts);
int fs_remove(directory_client *client, const char *path);
int fs_remove_all(directory_client *client, const char *path);
int fs_rename(directory_client *client, const char *old_path, const char *new_path);
int fs_status(directory_client *client, const char *path, struct file_status *status);
struct directory_entry *fs_directory_entries(directory_client *client, const char *path);
struct directory_entry *fs_recursive_directory_entries(directory_client *client, const char *path);
int fs_dstatus(directory_client *client, const char *path, struct data_status *status);
int fs_is_regular_file(directory_client *client, const char *path);
int fs_is_directory(directory_client *client, const char *path);

int fs_sync(directory_client *client, const char *path, const char *backing_path);
int fs_dump(directory_client *client, const char *path, const char *backing_path);
int fs_load(directory_client *client, const char *path, const char *backing_path);

#ifdef __cplusplus
}
#endif

#endif //MEMORYMUX_DIRECTORY_CLIENT_WRAPPER_H
