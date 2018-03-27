#ifndef ELASTICMEM_CHAIN_MODULE_H
#define ELASTICMEM_CHAIN_MODULE_H

#include <atomic>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include <libcuckoo/cuckoohash_map.hh>
#include <shared_mutex>
#include "service/block_client.h"
#include "manager/detail/block_name_parser.h"
#include "block.h"

namespace elasticmem {
namespace storage {

class chain_exception : public std::exception {
 public:
  explicit chain_exception(std::string msg) : msg_(std::move(msg)) {}

  char const *what() const noexcept override {
    return msg_.c_str();
  }
 private:
  std::string msg_;
};

enum chain_role {
  singleton = 0,
  head = 1,
  mid = 2,
  tail = 3
};

class chain_block_ctx {
 public:
  chain_block_ctx() = default;

  explicit chain_block_ctx(const std::string &block_name) {
    reset(block_name);
  }

  void reset(const std::string &block_name) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    client_.disconnect();
    if (block_name != "nil") {
      auto bid = block_name_parser::parse(block_name);
      auto host = bid.host;
      auto port = bid.service_port;
      auto block_id = bid.id;
      client_.connect(host, port, block_id);
    }
  }

  void run_command(std::vector<std::string> &result,
                   int64_t seq_no,
                   int32_t op_id,
                   const std::vector<std::string> &args) {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    client_.run_command(result, seq_no, op_id, args);
  }

  int32_t send_command(int64_t seq_no, int32_t op_id, const std::vector<std::string> &args) {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return client_.send_command(seq_no, op_id, args);
  }

  void recv_command_result(int32_t seq_id, std::vector<std::string> &result) {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return client_.recv_command_result(seq_id, result);
  }

  bool is_connected() {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return client_.is_connected();
  }

  std::string endpoint() {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    return client_.endpoint();
  }

 private:
  std::shared_mutex mtx_;
  block_client client_;
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
    return role() == chain_role::head || role() == chain_role::singleton;
  }

  bool is_tail() const {
    return role() == chain_role::tail || role() == chain_role::singleton;
  }

  void reset_next(const std::string &next_block) {
    next_->reset(next_block);
  }

  std::shared_ptr<chain_block_ctx> next() const {
    return next_;
  }

  void add_pending(int64_t sn, int op_id, const std::vector<std::string> &args) {
    pending_.insert(sn, chain_op{op_id, args});
  }

  void remove_pending(int64_t sn) {
    pending_.erase(sn);
  }

  void resend_pending();

  virtual void forward_all() = 0;

  int64_t incr_seq_no() {
    return seq_no_.fetch_add(1L);
  }

  void run_command_chain(std::vector<std::string> &_return,
                         int64_t seq_no,
                         int32_t oid,
                         const std::vector<std::string> &args);

 private:
  chain_role role_{singleton};
  std::atomic<int64_t> seq_no_{0};
  std::shared_ptr<chain_block_ctx> next_{nullptr};
  cuckoohash_map<int64_t, chain_op> pending_{};
};

}
}

#endif //ELASTICMEM_CHAIN_MODULE_H
