#ifndef MEMORYMUX_KV_CLIENT_WRAPPER_H
#define MEMORYMUX_KV_CLIENT_WRAPPER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "../../directory/client/directory_client_wrapper.h"

typedef void kv_client;
typedef kv_client* kv_client_ptr;

int destroy_kv(kv_client *client);

int kv_refresh(kv_client *client);

int kv_get_status(kv_client *client, struct data_status *status);

char *kv_put(kv_client *client, const char *key, const char *value);
char *kv_get(kv_client *client, const char *key);
char *kv_update(kv_client *client, const char *key, const char *value);
char *kv_remove(kv_client *client, const char *key);

#ifdef __cplusplus
}
#endif

#endif //MEMORYMUX_KV_CLIENT_WRAPPER_H
