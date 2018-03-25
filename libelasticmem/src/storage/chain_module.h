#ifndef ELASTICMEM_CHAIN_MODULE_H
#define ELASTICMEM_CHAIN_MODULE_H

#include <atomic>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include <libcuckoo/cuckoohash_map.hh>
#include "service/block_client.h"
#include "manager/detail/block_name_parser.h"
#include "block.h"

namespace elasticmem {
namespace storage {

enum chain_role {
  singleton = 0,
  head = 1,
  mid = 2,
  tail = 3
};

struct chain_block_ctx {
  std::string host;
  int port{-1};
  int block_id{-1};
  block_client client{};

  chain_block_ctx() = default;

  explicit chain_block_ctx(const std::string &block_name) {
    reset(block_name);
  }

  void reset(const std::string &block_name) {
    disconnect();
    if (block_name != "nil") {
      auto bid = block_name_parser::parse(block_name);
      host = bid.host;
      port = bid.service_port;
      block_id = bid.id;
      connect();
    }
  }

  void connect() {
    client.connect(host, port, block_id);
  }

  void disconnect() {
    client.disconnect();
  }

  bool is_connected() {
    return client.is_connected();
  }
};

struct chain_op {
  int32_t op_id;
  std::vector<std::string> args;
};

class chain_module : public block {
 public:
  chain_module(const std::string &block_name,
               const std::vector<block_op> &block_ops)
      : block(block_ops, block_name), next_(std::make_shared<chain_block_ctx>("nil")) {}

  void role(chain_role role) {
    role_ = role;
  }

  chain_role role() const {
    return role_;
  }

  bool is_head() const {
    return role_ == chain_role::head || role_ == chain_role::singleton;
  }

  bool is_tail() const {
    return role_ == chain_role::tail || role_ == chain_role::singleton;
  }

  void reset_next(const std::string &next_block) {
    next_->reset(next_block);
  }

  void add_pending(int32_t sn, int op_id, const std::vector<std::string> &args) {
    pending_.insert(sn, chain_op{op_id, args});
  }

  void remove_pending(int32_t sn) {
    pending_.erase(sn);
  }

  void resend_pending() {
    auto ops = pending_.lock_table();
    for (auto op: ops) {
      std::vector<std::string> response;
      next_->client.run_command(response, op.second.op_id, op.second.args);
    }
  }

  void run_command_chain(std::vector<std::string> &_return, int32_t oid, const std::vector<std::string> &args);

 private:
  chain_role role_{singleton};
  std::shared_ptr<chain_block_ctx> next_{nullptr};
  cuckoohash_map<int32_t, chain_op> pending_{};
};

}
}

#endif //ELASTICMEM_CHAIN_MODULE_H
