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

/*
 * Chain roles
 * We mark out the special rule singleton if there is only one block for the chain
 * i.e. we don't use chain replication in this case
 * The head would deal with update request and the tail would deal with query request
 * and also generate the respond
 */

enum chain_role {
  singleton = 0,
  head = 1,
  mid = 2,
  tail = 3
};

/*
 * Chain operation
 */

struct chain_op {
  /* Operation sequence identifier */
  sequence_id seq;
  /* Operation identifier */
  int32_t op_id;
  /* Operation arguments */
  std::vector<std::string> args;
};

/* Next block connection class */
class next_block_cxn {
 public:
  next_block_cxn() = default;

  /**
   * @brief Constructor
   * @param block_name Next block name
   */

  explicit next_block_cxn(const std::string &block_name) {
    reset(block_name);
  }

  /**
   * @brief Destructor
   */

  ~next_block_cxn() {
    client_.disconnect();
  }

  /**
   * @brief Reset block
   * Disconnect the current chain request client and connect to the new block
   * @param block_name Block name
   * @return Client protocol
   */

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

  /**
   * @brief Request an operation
   * @param seq Request sequence identifier
   * @param op_id Operation identifier
   * @param args Operation arguments
   */

  void request(const sequence_id &seq,
               int32_t op_id,
               const std::vector<std::string> &args) {
    std::shared_lock<std::shared_mutex> lock(mtx_);
    client_.request(seq, op_id, args);
  }

  /**
   * @brief Run command on next block
   * @param result Result
   * @param cmd_id Command identifier
   * @param args Command arguments
   */

  void run_command(std::vector<std::string> &result, int32_t cmd_id, const std::vector<std::string> &args) {
    std::unique_lock<std::shared_mutex> lock(mtx_);
    client_.run_command(result, cmd_id, args);
  }

 private:
  /* Operation mutex */
  std::shared_mutex mtx_;
  /* Chain request client */
  chain_request_client client_;
};

/* Previous block connection class */
class prev_block_cxn {
 public:
  prev_block_cxn() = default;

  /**
   * @brief Constructor
   * @param prot Protocol
   */

  explicit prev_block_cxn(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot) {
    reset(std::move(prot));
  }

  /**
   * @brief Reset protocol
   * @param prot Protocol
   */

  void reset(std::shared_ptr<::apache::thrift::protocol::TProtocol> prot) {
    client_.reset_prot(std::move(prot));
  }

  /**
   * @brief Acknowledge previous block
   * @param seq Chain Sequence identifier
   */

  void ack(const sequence_id &seq) {
    client_.ack(seq);
  }

  /**
   * @brief Check if chain response client is set
   * @return Bool value, true if set
   */

  bool is_set() const {
    return client_.is_set();
  }

 private:
  /* Chain response client */
  chain_response_client client_;
};

/* Chain module class
 * Inherited from block */
class chain_module : public block {
 public:
  /* Class chain response handler
   * Inherited from chain response serviceIf class */
  class chain_response_handler : public chain_response_serviceIf {
   public:
    /**
     * @brief Constructor
     * @param module Chain module
     */

    explicit chain_response_handler(chain_module *module) : module_(module) {}

    /**
     * @brief Chain acknowledgement
     * @param seq Operation sequence identifier
     */

    void chain_ack(const sequence_id &seq) override {
      module_->ack(seq);
    }

   private:
    /* Chain module */
    chain_module *module_;
  };

  /**
   * @brief Constructor
   * @param block_name Block name
   * @param block_ops Block operations
   */

  chain_module(const std::string &block_name,
               const std::vector<block_op> &block_ops)
      : block(block_ops, block_name),
        next_(std::make_unique<next_block_cxn>("nil")),
        prev_(std::make_unique<prev_block_cxn>()),
        pending_(0) {}

  /**
   * @brief Destructor
   */

  ~chain_module() {
    next_->reset("nil");
    if (response_processor_.joinable())
      response_processor_.join();
  }

  /**
   * @brief Setup a chain module and start the processor thread
   * @param path Block path
   * @param slot_begin Hash begin slot
   * @param slot_end Hash end slot
   * @param chain Replica chain block names
   * @param auto_scale Auto scaling boolean
   * @param role Chain module role
   * @param next_block_name Next block name
   */

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

  /**
   * @brief Set block to be exporting
   * @param target_block Export target block
   * @param slot_begin Begin slot
   * @param slot_end End slot
   */

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

  /**
   * @brief Set block to be importing
   * @param slot_begin Begin slot
   * @param slot_end End slot
   */

  void set_importing(int32_t slot_begin, int32_t slot_end) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    state_ = block_state::importing;
    import_slot_range_.first = slot_begin;
    import_slot_range_.second = slot_end;
  }

  /**
   * @brief Set block to regular after exporting slot
   * @param slot_begin Begin slot
   * @param slot_end End slot
   */

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

  /**
   * @brief Set chain module role
   * @param role Role
   */

  void role(chain_role role) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    role_ = role;
  }

  /**
   * @brief Fetch chain module role
   * @return Role
   */

  chain_role role() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return role_;
  }

  /**
   * @brief Set replica chain block names
   * @param chain Chain block names
   */

  void chain(const std::vector<std::string> &chain) {
    std::unique_lock<std::shared_mutex> lock(metadata_mtx_);
    chain_ = chain;
  }

  /**
   * @brief Fetch replica chain block names
   * @return Replica chain block names
   */

  const std::vector<std::string> &chain() {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return chain_;
  }

  /**
   * @brief Check if chain module role is head
   * @return Bool value, true if chain role is head or singleton
   */

  bool is_head() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return role() == chain_role::head || role() == chain_role::singleton;
  }

  /**
   * @brief Check if chain module role is tail
   * @return Bool value, true if chain role is tail or singleton
   */

  bool is_tail() const {
    std::shared_lock<std::shared_mutex> lock(metadata_mtx_);
    return role() == chain_role::tail || role() == chain_role::singleton;
  }

  /**
   * @brief Check if previous chain module is set
   * @return Bool value, true if set
   */

  bool is_set_prev() const {
    return prev_->is_set(); // Does not require a lock since only one thread calls this at a time
  }

  /**
   * @brief Reset previous block
   * @param prot Protocol
   */

  void reset_prev(const std::shared_ptr<::apache::thrift::protocol::TProtocol> &prot) {
    prev_->reset(prot); // Does not require a lock since only one thread calls this at a time
  }

  /**
   * @brief Reset next block
   * @param next_block Next block
   */

  void reset_next(const std::string &next_block);

  /**
   * @brief Reset next block and start up response processor thread
   * @param next_block Next block
   */

  void reset_next_and_listen(const std::string &next_block);

  /**
   * @brief Run command on next block
   * @param result Command result
   * @param oid Operation identifier
   * @param args Operation arguments
   */

  void run_command_on_next(std::vector<std::string> &result, int32_t oid, const std::vector<std::string> &args) {
    next_->run_command(result, oid, args);
  }

  /**
   * @brief Add request to pending
   * @param seq Request sequence identifier
   * @param op_id Operation identifier
   * @param args Operation arguments
   */

  void add_pending(const sequence_id &seq, int op_id, const std::vector<std::string> &args) {
    pending_.insert(seq.server_seq_no, chain_op{seq, op_id, args});
  }

  /**
   * @brief Remove a pending request
   * @param seq Sequence identifier
   */

  void remove_pending(const sequence_id &seq) {
    pending_.erase(seq.server_seq_no);
  }

  /**
   * @brief Resend the pending request
   */

  void resend_pending();

  /**
   * @brief Virtual function for forwarding all
   */

  virtual void forward_all() = 0;

  /**
   * @brief Request for the first time
   * @param seq Sequence identifier
   * @param oid Operation identifier
   * @param args Operation arguments
   */

  void request(sequence_id seq, int32_t oid, const std::vector<std::string> &args);

  /**
   * @brief Chain request
   * @param seq Sequence identifier
   * @param oid Operation identifier
   * @param args Operation arguments
   */

  void chain_request(const sequence_id &seq, int32_t oid, const std::vector<std::string> &args);

  /**
   * @brief Acknowledge the previous block
   * @param seq Sequence identifier
   */

  void ack(const sequence_id &seq);

 protected:
  /* Request mutex */
  std::mutex request_mtx_;
  /* Role of chain module */
  chain_role role_{singleton};
  /* Chain sequence number */
  int64_t chain_seq_no_{0};
  /* Next block connection */
  std::unique_ptr<next_block_cxn> next_{nullptr};
  /* Previous block connection */
  std::unique_ptr<prev_block_cxn> prev_{nullptr};
  /* Replica chain block names */
  std::vector<std::string> chain_;
  /* Response processor thread */
  std::thread response_processor_;
  /* Pending operations */
  cuckoohash_map<int64_t, chain_op> pending_;
};

}
}

#endif //MMUX_CHAIN_MODULE_H