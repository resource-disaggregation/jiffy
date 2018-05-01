#include <iostream>
#include "subscription_map.h"

namespace elasticmem {
namespace storage {

subscription_map::subscription_map() : pool_(std::thread::hardware_concurrency()) {}

void subscription_map::add_subscriptions(const std::vector<std::string> &ops,
                                         std::shared_ptr<subscription_serviceClient> client) {
  std::lock_guard<std::mutex> lock{mtx_};
  for (const auto &op: ops)
    subs_[op].insert(client);
  client->success(response_type::subscribe, ops);
}

void subscription_map::remove_subscriptions(const std::vector<std::string> &ops,
                                            std::shared_ptr<subscription_serviceClient> client, bool inform) {
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
    client->success(response_type::unsubscribe, ops);
}

void subscription_map::notify(const std::string &op, const std::string &msg) {
  std::lock_guard<std::mutex> lock{mtx_};
  auto clients = subs_[op];
  std::vector<std::future<void>> futures(clients.size());
  std::size_t i = 0;
  for (const auto &client: clients) {
    futures[i++] = pool_.push([&](int) -> void { client->notification(op, msg); });
  }

  for (const auto& f: futures) {
    f.wait();
  }
}

void subscription_map::clear() {
  std::lock_guard<std::mutex> lock{mtx_};
  subs_.clear();
}

}
}