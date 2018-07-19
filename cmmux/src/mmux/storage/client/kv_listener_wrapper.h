#ifndef MEMORYMUX_KV_LISTENER_WRAPPER_H
#define MEMORYMUX_KV_LISTENER_WRAPPER_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void kv_listener;
typedef kv_listener* kv_listener_ptr;

struct notification_t {
  char* op;
  char* arg;
};

int destroy_listener(kv_listener *listener);
int kv_subscribe(kv_listener *listener, const char **ops, size_t n_ops);
int kv_unsubscribe(kv_listener *listener, const char **ops, size_t n_ops);
int kv_get_notification(kv_listener *listener, int64_t timeout_ms, struct notification_t *n);

#ifdef __cplusplus
}
#endif

#endif //MEMORYMUX_KV_LISTENER_WRAPPER_H
