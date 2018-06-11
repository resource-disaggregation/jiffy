#include <iostream>
#include "notification_service_handler.h"

using namespace ::apache::thrift::protocol;

namespace mmux {
namespace storage {

notification_service_handler::notification_service_handler(std::shared_ptr<TProtocol> oprot,
                                                           std::vector<std::shared_ptr<chain_module>> &blocks)
    : oprot_(std::move(oprot)), client_(new subscription_serviceClient(oprot_)), blocks_(blocks) {}

void notification_service_handler::subscribe(const int32_t block_id, const std::vector<std::string> &ops) {
  std::vector<std::string> added;
  for (auto op: ops) {
    local_subs_.insert(std::make_pair(block_id, op));
    added.push_back(op);
  }
  blocks_[block_id]->subscriptions().add_subscriptions(added, client_);
}

void notification_service_handler::unsubscribe(int32_t block_id, const std::vector<std::string> &ops) {
  std::vector<std::string> removed;
  bool inform = block_id != -1;
  if (ops.empty()) {
    for (const auto &sub: local_subs_) {
      if (block_id == -1)
        block_id = sub.first;
      removed.push_back(sub.second);
    }
    local_subs_.clear();
  } else {
    for (auto op: ops) {
      auto it = local_subs_.find(std::make_pair(block_id, op));
      if (it != local_subs_.end()) {
        if (block_id == -1)
          block_id = it->first;
        local_subs_.erase(it);
        removed.push_back(it->second);
      }
    }
  }
  if (!removed.empty())
    blocks_[block_id]->subscriptions().remove_subscriptions(removed, client_, inform);
}

}
}