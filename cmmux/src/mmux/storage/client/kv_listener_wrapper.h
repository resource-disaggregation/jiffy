#ifndef MEMORYMUX_KV_LISTENER_WRAPPER_H
#define MEMORYMUX_KV_LISTENER_WRAPPER_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void kv_listener;

struct notification_t {
  char* op;
  char* arg;
};

void destroy_listener(kv_listener *listener);
void kv_subscribe(kv_listener* listener, const char** ops, size_t n_ops);
void kv_unsubscribe(kv_listener* listener, const char** ops, size_t n_ops);
struct notification_t kv_get_notification(kv_listener* listener, int64_t timeout_ms);

#ifdef __cplusplus
}
#endif

#endif //MEMORYMUX_KV_LISTENER_WRAPPER_H
