#ifndef MEMORYMUX_KV_CLIENT_WRAPPER_H
#define MEMORYMUX_KV_CLIENT_WRAPPER_H

#include "../../directory/client/directory_client_wrapper.h"
#ifdef __cplusplus
extern "C" {
#endif

typedef void kv_client;
typedef void locked_kv_client;

void destroy_kv(kv_client *client);

void kv_refresh(kv_client *client);

struct data_status kv_get_status(kv_client *client);

char *kv_put(kv_client *client, const char *key, const char *value);
char *kv_get(kv_client *client, const char *key);
char *kv_update(kv_client *client, const char *key, const char *value);
char *kv_remove(kv_client *client, const char *key);

#ifdef __cplusplus
}
#endif

#endif //MEMORYMUX_KV_CLIENT_WRAPPER_H
