#include <iostream>
#include "subscription_map.h"

namespace mmux {
namespace storage {

subscription_map::subscription_map() = default;

void subscription_map::add_subscriptions(const std::vector<std::string> &ops,
                                         std::shared_ptr<subscription_serviceClient> client) {
  std::lock_guard<std::mutex> lock{mtx_};
  for (const auto &op: ops)
    subs_[op].insert(client);
  client->control(response_type::subscribe, ops, "");
}

void subscription_map::remove_subscriptions(const std::vector<std::string> &ops,
                                            std::shared_ptr<subscription_serviceClient> client,
                                            bool inform) {
  std::lock_guard<std::mutex> lock{mtx_};
  for (const auto &op: ops) {
    auto &clients = subs_[op];
    auto it = clients.find(client);
    if (it != clients.end()) {
      clients.erase(it);
      if (clients.empty())
        subs_.erase(op);
    }
  }
  if (inform)
    client->control(response_type::unsubscribe, ops, "");
}

void subscription_map::notify(const std::string &op, const std::string &msg) {
  std::lock_guard<std::mutex> lock{mtx_};
  for (const auto &client: subs_[op]) {
    client->notification(op, msg);
  }
}

void subscription_map::clear() {
  std::lock_guard<std::mutex> lock{mtx_};
  subs_.clear();
}

}
}