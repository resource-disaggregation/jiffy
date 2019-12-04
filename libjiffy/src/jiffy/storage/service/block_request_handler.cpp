#include "block_request_handler.h"
#include "jiffy/utils/logger.h"
namespace jiffy {
namespace storage {
using namespace std;
block_request_handler::block_request_handler(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot,
                                             std::atomic<int64_t> &client_id_gen,
                                             std::map<int, std::shared_ptr<block>> &blocks)
    : prot_(std::move(prot)),
      client_(std::make_shared<block_response_client>(prot_)),
      notification_client_(std::make_shared<notification_response_client>(prot_)),
      registered_block_id_(-1),
      registered_client_id_(-1),
      client_id_gen_(client_id_gen),
      blocks_(blocks) {}

int64_t block_request_handler::get_client_id() {
  return client_id_gen_.fetch_add(1L);
}

void block_request_handler::register_client_id(const int32_t block_id, const int64_t client_id) {
  registered_client_id_ = client_id;
  registered_block_id_ = block_id;
  blocks_[static_cast<std::size_t>(block_id)]->impl()->clients().add_client(client_id, client_);
}

void block_request_handler::command_request(const sequence_id &seq,
                                            const int32_t block_id,
                                            const std::vector<std::string> &args) {
  blocks_[static_cast<std::size_t>(block_id)]->impl()->request(seq, args);
}

int32_t block_request_handler::registered_block_id() const {
  return registered_block_id_;
}

int64_t block_request_handler::registered_client_id() const {
  return registered_client_id_;
}

void block_request_handler::chain_request(const sequence_id &seq,
                                          const int32_t block_id,
                                          const std::vector<std::string> &args) {
  const auto &b = blocks_[static_cast<std::size_t>(block_id)];
  if (!b->impl()->is_set_prev()) {
    b->impl()->reset_prev(prot_);
  }
  b->impl()->chain_request(seq, args);
}

void block_request_handler::run_command(std::vector<std::string> &_return,
                                        const int32_t block_id,
                                        const std::vector<std::string> &args) {
  blocks_[static_cast<std::size_t>(block_id)]->impl()->run_command(_return, args);
  blocks_[static_cast<std::size_t>(block_id)]->impl()->notify(args);
}

void block_request_handler::subscribe(int32_t block_id,
                                      const std::vector<std::string> &ops) {
  std::vector<std::string> added;
  for (auto op: ops) {
    local_subs_.insert(std::make_pair(block_id, op));
    added.push_back(op);
  }
  blocks_[static_cast<std::size_t>(block_id)]->impl()->subscriptions().add_subscriptions(added, notification_client_);
}

void block_request_handler::unsubscribe(int32_t block_id,
                                        const std::vector<std::string> &ops) {
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
    for (const auto& op: ops) {
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
    blocks_[static_cast<std::size_t>(block_id)]->impl()->subscriptions().remove_subscriptions(removed, notification_client_, inform);
}

}
}
