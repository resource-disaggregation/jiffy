#ifndef MEMORYMUX_CMMUX_CLIENT_H
#define MEMORYMUX_CMMUX_CLIENT_H

#include <stdint.h>
#include "../directory/client/directory_client_wrapper.h"
#include "../storage/client/kv_client_wrapper.h"
#include "../storage/client/kv_listener_wrapper.h"

#ifdef __cplusplus
extern "C" {
#endif

#define PINNED 0x01
#define STATIC_PROVISIONED 0x02
#define MAPPED 0x04

typedef void mmux_client;
typedef mmux_client* mmux_client_ptr;

mmux_client *create_mmux_client(const char *host, int32_t dir_port, int32_t lease_port);
int destroy_mmux_client(mmux_client *client);

int mmux_begin_scope(mmux_client *client, const char *path);
int mmux_end_scope(mmux_client *client, const char *path);

directory_client *mmux_get_fs(mmux_client *client);

kv_client *mmux_create(mmux_client *client,
                       const char *path,
                       const char *backing_path,
                       size_t num_blocks,
                       size_t chain_length,
                       int32_t flags);
kv_client *mmux_open(mmux_client *client, const char *path);
kv_client *mmux_open_or_create(mmux_client *client,
                               const char *path,
                               const char *backing_path,
                               size_t num_blocks,
                               size_t chain_length,
                               int32_t flags);
kv_listener *mmux_listen(mmux_client *client, const char *path);
int mmux_close(mmux_client *client, const char *path);
int mmux_remove(mmux_client *client, const char *path);
int mmux_sync(mmux_client *client, const char *path, const char *backing_path);
int mmux_dump(mmux_client *client, const char *path, const char *backing_path);
int mmux_load(mmux_client *client, const char *path, const char *backing_path);

#ifdef __cplusplus
}
#endif

#endif //MEMORYMUX_CMMUX_CLIENT_H
