#include <mmux/storage/client/kv_listener.h>
#include <string.h>
#include "kv_listener_wrapper.h"

int destroy_listener(kv_listener *listener) {
  try {
    mmux::storage::kv_listener *l = static_cast<mmux::storage::kv_listener *>(listener);
    delete l;
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int kv_subscribe(kv_listener *listener, const char **ops, size_t n_ops) {
  try {
    mmux::storage::kv_listener *l = static_cast<mmux::storage::kv_listener *>(listener);
    std::vector<std::string> args;
    for (size_t i = 0; i < n_ops; i++)
      args.push_back(std::string(ops[i]));
    l->subscribe(args);
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int kv_unsubscribe(kv_listener *listener, const char **ops, size_t n_ops) {
  try {
    mmux::storage::kv_listener *l = static_cast<mmux::storage::kv_listener *>(listener);
    std::vector<std::string> args;
    for (size_t i = 0; i < n_ops; i++)
      args.push_back(std::string(ops[i]));
    l->unsubscribe(args);
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}

int kv_get_notification(kv_listener *listener, int64_t timeout_ms, struct notification_t *n) {
  try {
    mmux::storage::kv_listener *l = static_cast<mmux::storage::kv_listener *>(listener);
    auto ret = l->get_notification(timeout_ms);
    n->op = strdup(ret.first.c_str());
    n->arg = strdup(ret.second.c_str());
  } catch (std::exception &e) {
    return -1;
  }
  return 0;
}
