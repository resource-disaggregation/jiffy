#ifndef MMUX_SUBSCRIPTION_MAP_H
#define MMUX_SUBSCRIPTION_MAP_H

#include <string>
#include <unordered_map>
#include <mutex>
#include "subscription_service.h"
namespace mmux {
namespace storage {

class subscription_map {
 public:
  subscription_map();

  void add_subscriptions(const std::vector<std::string> &ops, std::shared_ptr<subscription_serviceClient> client);

  void remove_subscriptions(const std::vector<std::string> &ops,
                              std::shared_ptr<subscription_serviceClient> client,
                              bool inform = true);

  void notify(const std::string &op, const std::string &msg);

  void clear();

 private:
  std::mutex mtx_{};
  std::map<std::string, std::set<std::shared_ptr<subscription_serviceClient>>> subs_{};
};

}
}

#endif //MMUX_SUBSCRIPTION_MAP_H
