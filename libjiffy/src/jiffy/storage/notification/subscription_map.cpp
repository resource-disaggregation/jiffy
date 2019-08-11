#include <iostream>
#include "subscription_map.h"

namespace jiffy {
namespace storage {

subscription_map::subscription_map() = default;

void subscription_map::add_subscriptions(const std::vector<std::string> &ops,
                                         const std::shared_ptr<notification_response_client>& client) {
  for (const auto &op: ops)
    subs_[op].insert(client);
  client->control(response_type::subscribe, ops, "");
}

void subscription_map::remove_subscriptions(const std::vector<std::string> &ops,
                                            const std::shared_ptr<notification_response_client>& client,
                                            bool inform) {
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
  if (op == "default_partition") return;
  for (const auto &client: subs_[op]) {
    client->notification(op, msg);
  }
}

void subscription_map::clear() {
  subs_.clear();
}

// TODO fix this function so that we could let the
// subscribed blocks know whenever the partition is destroyed
void subscription_map::end_connections() {
  for (const auto &sub: subs_) {
    for (const auto &client: sub.second) {
      client->notification("error", "!block_moved");
    }
  }
}

}
}
