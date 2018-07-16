#include <mmux/storage/client/kv_listener.h>
#include <string.h>
#include "kv_listener_wrapper.h"

void destroy_listener(kv_listener *listener) {
  mmux::storage::kv_listener *l = static_cast<mmux::storage::kv_listener *>(listener);
  delete l;
}

void kv_subscribe(kv_listener *listener, const char **ops, size_t n_ops) {
  mmux::storage::kv_listener *l = static_cast<mmux::storage::kv_listener *>(listener);
  std::vector<std::string> args;
  for (size_t i = 0; i < n_ops; i++)
    args.push_back(std::string(ops[i]));
  l->subscribe(args);
}

void kv_unsubscribe(kv_listener *listener, const char **ops, size_t n_ops) {
  mmux::storage::kv_listener *l = static_cast<mmux::storage::kv_listener *>(listener);
  std::vector<std::string> args;
  for (size_t i = 0; i < n_ops; i++)
    args.push_back(std::string(ops[i]));
  l->unsubscribe(args);
}

struct notification_t kv_get_notification(kv_listener *listener, int64_t timeout_ms) {
  mmux::storage::kv_listener *l = static_cast<mmux::storage::kv_listener *>(listener);
  auto ret = l->get_notification(timeout_ms);
  struct notification_t n;
  n.op = strdup(ret.first.c_str());
  n.arg = strdup(ret.second.c_str());
  return n;
}
