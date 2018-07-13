#ifndef MMUX_CHAIN_MODULE_H
#define MMUX_CHAIN_MODULE_H

#include <atomic>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <libcuckoo/cuckoohash_map.hh>
#include <shared_mutex>
#include "client/block_client.h"
#include "manager/detail/block_name_parser.h"
#include "block.h"
#include "service/chain_request_service.h"
#include "service/chain_request_client.h"
#include "service/chain_response_client.h"
#include "notification/subscription_map.h"
#include "service/block_response_client_map.h"

namespace mmux {
namespace storage {

enum chain_role {
  singleton = 0,
  head = 1,
  mid = 2,
  tail = 3
};

struct chain_op {
  sequence_id seq;
  int32_t op_id;
  std::vector<std::string> args;
};

class next_block_cxn {
 public:
  next_block_cxn() = default;

  explicit next_block_cxn(const std::string &block_name) {
    reset(block_name);
  }

  ~next_block_cxn() {
    client_.disconnect();
  }

  std::shared_ptr<apache::thrift::protocol::TProtocol> reset(const std::string &block_name) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    client_.disconnect();
    if (block_name != "nil") {
      auto bid = block_name_parser::parse(block_name);
      auto host = bid.host;
      auto port = bid.chain_port;
      auto block_id = bid.id;
      client_.connect(host, port, block_id);
    }
    return client_.protocol();
  }

  void request(const sequence_id &seq,
               int32_t op_id,
               const std::vector<std::string> &args) {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    client_.request(seq, op_id, args);
  }

  void run_command(std::vector<std::string> &result, int32_t cmd_id, const std::vector<std::string> &args) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    client_.run_command(result, cmd_id, args);
  }

 private:
  std::shared_mutex mtx_;
  chain_request_client client_;
};

class prev_block_cxn {
 public:
  prev_block_cxn() = default;

  explicit prev_block_cxn(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot) {
    reset(std::move(prot));
  }

  void reset(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot) {
    client_.reset_prot(std::move(prot));
  }

  void ack(const sequence_id &seq) {
    client_.ack(seq);
  }

  bool is_set() const {
    return client_.is_set();
  }

 private:
  chain_response_client client_;
};

class chain_module : public block {
 public:
  class chain_response_handler : public chain_response_serviceIf {
   public:
    explicit chain_response_handler(chain_module *module) : module_(module) {}

    void chain_ack(const sequence_id &seq) override {
      module_->ack(seq);
    }

   private:
    chain_module *module_;
  };

  chain_module(const std::string &block_name,
               const std::vector<block_op> &block_ops)
      : block(block_ops, block_name),
        next_(std::make_unique<next_block_cxn>("nil")),
        prev_(std::make_unique<prev_block_cxn>()),
        pending_(0) {}

  ~chain_module() {
    next_->reset("nil");
    if (response_processor_.joinable())
      response_processor_.join();
  }

  void setup(const std::string &path,
             int32_t slot_begin,
             int32_t slot_end,
             const std::vector<std::string> &chain,
             bool auto_scale,
             const chain_role &role,
             const std::string &next_block_name) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    path_ = path;
    slot_range_.first = slot_begin;
    slot_range_.second = slot_end;
    chain_ = chain;
    role_ = role;
    auto_scale_ = auto_scale;
    auto protocol = next_->reset(next_block_name);
    if (protocol && role_ != chain_role::tail) {
      auto handler = std::make_shared<chain_response_handler>(this);
      auto processor = std::make_shared<chain_response_serviceProcessor>(handler);
      if (response_processor_.joinable())
        response_processor_.join();
      response_processor_ = std::thread([processor, protocol] {
        while (true) {
          try {
            if (!processor->process(protocol, protocol, nullptr)) {
              break;
            }
          } catch (std::exception &e) {
            break;
          }
        }
      });
    }
  }

  void set_exporting(const std::vector<std::string> &target_block, int32_t slot_begin, int32_t slot_end) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    state_ = block_state::exporting;
    export_target_ = target_block;
    export_target_str_ = "";
    for (const auto &block: target_block) {
      export_target_str_ += (block + "!");
    }
    export_target_str_.pop_back();
    export_slot_range_.first = slot_begin;
    export_slot_range_.second = slot_end;
  }

  void set_importing(int32_t slot_begin, int32_t slot_end) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    state_ = block_state::importing;
    import_slot_range_.first = slot_begin;
    import_slot_range_.second = slot_end;
  }

  void set_regular(int32_t slot_begin, int32_t slot_end) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    state_ = block_state::regular;
    slot_range_.first = slot_begin;
    slot_range_.second = slot_end;
    export_slot_range_.first = 0;
    export_slot_range_.second = -1;
    import_slot_range_.first = 0;
    import_slot_range_.second = -1;
  }

  void role(chain_role role) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    role_ = role;
  }

  chain_role role() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return role_;
  }

  void chain(const std::vector<std::string> &chain) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    chain_ = chain;
  }

  const std::vector<std::string> &chain() {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return chain_;
  }

  bool is_head() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return role() == chain_role::head || role() == chain_role::singleton;
  }

  bool is_tail() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return role() == chain_role::tail || role() == chain_role::singleton;
  }

  bool is_set_prev() const {
    return prev_->is_set(); // Does not require a lock since only one thread calls this at a time
  }

  void reset_prev(const std::shared_ptr<::apache::thrift::protocol::TProtocol> &prot) {
    prev_->reset(prot); // Does not require a lock since only one thread calls this at a time
  }

  void reset_next(const std::string &next_block);

  void reset_next_and_listen(const std::string &next_block);

  void run_command_on_next(std::vector<std::string> &result, int32_t oid, const std::vector<std::string> &args) {
    next_->run_command(result, oid, args);
  }

  void add_pending(const sequence_id &seq, int op_id, const std::vector<std::string> &args) {
    pending_.insert(seq.server_seq_no, chain_op{seq, op_id, args});
  }

  void remove_pending(const sequence_id &seq) {
    pending_.erase(seq.server_seq_no);
  }

  void resend_pending();

  virtual void forward_all() = 0;

  void request(sequence_id seq, int32_t oid, const std::vector<std::string> &args);
  void chain_request(const sequence_id &seq, int32_t oid, const std::vector<std::string> &args);
  void ack(const sequence_id &seq);

 protected:
  std::mutex request_mtx_;
  chain_role role_{singleton};
  int64_t chain_seq_no_{0};
  std::unique_ptr<next_block_cxn> next_{nullptr};
  std::unique_ptr<prev_block_cxn> prev_{nullptr};
  std::vector<std::string> chain_;
  std::thread response_processor_;
  cuckoohash_map<int64_t, chain_op> pending_;
};

}
}

#endif //MMUX_CHAIN_MODULE_H
